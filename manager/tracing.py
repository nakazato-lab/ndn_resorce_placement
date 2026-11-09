"""Configure OTLP trace export for this process."""
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_tracing(service_name):
    if not os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
        return None
    provider = TracerProvider(resource=Resource.create({
        'service.name': os.getenv('OTEL_SERVICE_NAME', service_name),
    }))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider
