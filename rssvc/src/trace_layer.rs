use hyper::{Request, Response};
use std::time::Duration;
use tower_http::trace;
use tracing::field;

#[derive(Default, Debug, Clone, Copy)]
pub struct MakeSpan;

impl<B> trace::MakeSpan<B> for MakeSpan {
    fn make_span(&mut self, req: &Request<B>) -> tracing::Span {
        let req_id = uuid::Uuid::new_v4();
        tracing::info_span!("request",
            id = %req_id.to_hyphenated(),
            method = %req.method(),
            uri = %req.uri(),
            version = ?req.version(),
            latency = field::Empty,
            status = field::Empty
        )
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct OnRequest;

impl<B> trace::OnRequest<B> for OnRequest {
    fn on_request(&mut self, _: &Request<B>, _: &tracing::Span) {
        tracing::info!("started processing request")
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct OnResponse;

impl<B> trace::OnResponse<B> for OnResponse {
    fn on_response(self, resp: &Response<B>, latency: Duration, span: &tracing::Span) {
        span.record("latency", &format_args!("{:?}", latency));
        span.record("status", &resp.status().as_u16());
        tracing::info!("finished processing request")
    }
}
