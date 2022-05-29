use futures::{Stream, StreamExt};
use std::pin::Pin;
use tonic::{transport::Server, Request, Response, Status};

use echo::echo_server::{Echo, EchoServer};
use echo::{EchoRequest, EchoResponse};

pub mod echo {
    tonic::include_proto!("echo");
}

#[derive(Debug, Default)]
pub struct ServerEcho {}

#[tonic::async_trait]
impl Echo for ServerEcho {
    async fn unary_echo(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        let message = request.into_inner().message;
        println!("unary echoing message: {:?}", message);
        Ok(Response::new(EchoResponse { message }))
    }

    type ServerStreamingEchoStream =
        Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

    async fn server_streaming_echo(
        &self,
        _: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn client_streaming_echo(
        &self,
        _: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type BidirectionalStreamingEchoStream =
        Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send + 'static>>;

    async fn bidirectional_streaming_echo(
        &self,
        request: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidirectionalStreamingEchoStream>, Status> {
        let mut stream = request.into_inner();

        let output = async_stream::try_stream! {
            while let Some(request) = stream.next().await {
                let request = request?;
                let msg = request.message;
                println!("bidi echoing message {:?}", msg);
                let response = EchoResponse {
                    message: msg,
                };

                yield response;
            }
        };

        Ok(Response::new(
            Box::pin(output) as Self::BidirectionalStreamingEchoStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let echo = ServerEcho::default();

    Server::builder()
        .add_service(EchoServer::new(echo))
        .serve(addr)
        .await?;

    Ok(())
}
