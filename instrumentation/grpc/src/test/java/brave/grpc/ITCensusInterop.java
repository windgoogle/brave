package brave.grpc;

import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.StrictCurrentTraceContext;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.netty.InternalNettyChannelBuilder;
import io.grpc.netty.InternalNettyServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcMeasureConstants;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tags;
import io.opencensus.testing.export.TestHandler;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.samplers.Samplers;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span;

import static brave.grpc.GreeterImpl.HELLO_REQUEST;
import static io.grpc.ServerInterceptors.intercept;
import static io.opencensus.trace.Tracing.getExportComponent;
import static io.opencensus.trace.Tracing.getTraceConfig;
import static org.assertj.core.api.Assertions.assertThat;

public class ITCensusInterop {

  final TestHandler testHandler = new TestHandler();

  @Before public void beforeClass() {
    getTraceConfig().updateActiveTraceParams(TraceParams.DEFAULT.toBuilder()
        .setSampler(Samplers.alwaysSample()).build());
    getExportComponent().getSpanExporter().registerHandler("test", testHandler);
  }

  /** See brave.http.ITHttp for rationale on using a concurrent blocking queue */
  BlockingQueue<Span> spans = new LinkedBlockingQueue<>();
  Tracing tracing = Tracing.newBuilder()
      .propagationFactory(GrpcPropagation.newFactory(B3Propagation.FACTORY))
      .spanReporter(spans::add)
      .currentTraceContext(new StrictCurrentTraceContext()).build();
  GrpcTracing grpcTracing = GrpcTracing.create(tracing);

  Server server;
  ManagedChannel client;

  @Test public void readsCensusPropagation() throws Exception {
    initServer(true); // trace server with brave
    initClient(false); // trace client with census

    GreeterGrpc.newBlockingStub(client).sayHello(HELLO_REQUEST);

    // this takes 5 seconds due to hard-coding in ExportComponentImpl
    SpanData clientSpan = testHandler.waitForExport(1).get(0);

    Span serverSpan = spans.take();
    assertThat(clientSpan.getContext().getTraceId().toLowerBase16())
        .isEqualTo(serverSpan.traceId());
    assertThat(clientSpan.getContext().getSpanId().toLowerBase16())
        .isEqualTo(serverSpan.parentId());
    assertThat(serverSpan.tags())
        .doesNotContainKeys("grpc.parent_method");
  }

  @Test public void readsCensusPropagation_withIncomingMethod() throws Exception {
    initServer(true); // trace server with brave
    initClient(false); // trace client with census

    try (Scope tagger = Tags.getTagger().emptyBuilder()
        .put(RpcMeasureConstants.RPC_METHOD, TagValue.create("edge.Ingress/InitialRoute"))
        .buildScoped()
    ) {
      GreeterGrpc.newBlockingStub(client).sayHello(HELLO_REQUEST);
    }

    // this takes 5 seconds due to hard-coding in ExportComponentImpl
    SpanData clientSpan = testHandler.waitForExport(1).get(0);

    Span serverSpan = spans.take();
    assertThat(clientSpan.getContext().getTraceId().toLowerBase16())
        .isEqualTo(serverSpan.traceId());
    assertThat(clientSpan.getContext().getSpanId().toLowerBase16())
        .isEqualTo(serverSpan.parentId());
  }

  @Test public void writesCensusPropagation() throws Exception {
    initServer(false); // trace server with census
    initClient(true); // trace client with brave

    GreeterGrpc.newBlockingStub(client).sayHello(HELLO_REQUEST);

    // this takes 5 seconds due to hard-coding in ExportComponentImpl
    SpanData serverSpan = testHandler.waitForExport(1).get(0);

    Span clientSpan = spans.take();
    assertThat(clientSpan.traceId())
        .isEqualTo(serverSpan.getContext().getTraceId().toLowerBase16());
    assertThat(clientSpan.id())
        .isEqualTo(serverSpan.getParentSpanId().toLowerBase16());
  }

  void initServer(boolean traceWithBrave) throws Exception {
    if (traceWithBrave) {
      NettyServerBuilder builder = (NettyServerBuilder) ServerBuilder.forPort(PickUnusedPort.get())
          .addService(intercept(new GreeterImpl(grpcTracing), grpcTracing.newServerInterceptor()));
      // TODO: track gRPC exposing this
      InternalNettyServerBuilder.setTracingEnabled(builder, false);
      server = builder.build();
    } else {
      server = ServerBuilder.forPort(PickUnusedPort.get())
          .addService(new GreeterImpl(null))
          .build();
    }
    server.start();
  }

  void initClient(boolean traceWithBrave) {
    if (traceWithBrave) {
      NettyChannelBuilder builder = (NettyChannelBuilder)
          ManagedChannelBuilder.forAddress("localhost", server.getPort())
              .intercept(grpcTracing.newClientInterceptor())
              .usePlaintext();
      // TODO: track gRPC exposing this
      InternalNettyChannelBuilder.setTracingEnabled(builder, false);
      client = builder.build();
    } else {
      client = ManagedChannelBuilder.forAddress("localhost", server.getPort())
          .usePlaintext()
          .build();
    }
  }

  @After public void close() throws Exception {
    if (client != null) {
      client.shutdown();
      client.awaitTermination(1, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.shutdown();
      server.awaitTermination(1, TimeUnit.SECONDS);
    }
    tracing.close();
  }
}
