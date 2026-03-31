import grpc

from trove import (
    instrument_pb2,
    instrument_service_pb2,
    instrument_service_pb2_grpc,
    position_pb2,
    position_service_pb2,
    position_service_pb2_grpc,
    market_data_pb2,
    market_data_service_pb2,
    market_data_service_pb2_grpc,
    filter_pb2,
)


class TroveGrpcClient:
    # Default max message size: 50MB (enough for large instrument/greeks batches)
    MAX_MESSAGE_SIZE = 50 * 1024 * 1024

    def __init__(self, host: str, port: int, secure: bool = True):
        options = [
            ("grpc.max_receive_message_length", self.MAX_MESSAGE_SIZE),
            ("grpc.max_send_message_length", self.MAX_MESSAGE_SIZE),
        ]

        if secure:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(
                host + ":" + str(port),
                credentials,
                options=options,
            )
        else:
            channel = grpc.insecure_channel(host + ":" + str(port), options=options)

        self.instrument_stub = instrument_service_pb2_grpc.InstrumentServiceStub(channel)
        self.position_stub = position_service_pb2_grpc.PositionServiceStub(channel)
        self.market_data_stub = market_data_service_pb2_grpc.MarketDataServiceStub(channel)

    class TroveServerUnavailableError(Exception):
        pass

    class UpsertPositionFailedPreconditionError(Exception):
        pass

    def get_instrument(self, instrument_id: str) -> instrument_pb2.Instrument:
        try:
            grpc_response = self.instrument_stub.GetInstrument(
                instrument_service_pb2.GetInstrumentRequest(
                    id=instrument_id,
                )
            )
            return grpc_response.instrument
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e

    def list_instruments(
        self, instrument_filter: instrument_pb2.InstrumentFilter
    ) -> list[instrument_pb2.Instrument]:
        try:
            grpc_response = self.instrument_stub.ListInstruments(
                instrument_service_pb2.ListInstrumentsRequest(
                    filter=instrument_filter,
                )
            )
            return grpc_response.instruments
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e

    def list_positions(
        self, position_filter: position_pb2.PositionFilter
    ) -> list[position_pb2.Position]:
        try:
            grpc_response = self.position_stub.ListPositions(
                position_service_pb2.ListPositionsRequest(
                    filter=position_filter,
                )
            )
            return grpc_response.positions
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e

    def get_market_data_batch(
        self, instrument_ids: list[str]
    ) -> dict[str, market_data_pb2.MarketData]:
        try:
            grpc_response = self.market_data_stub.GetMarketDataBatch(
                market_data_service_pb2.GetMarketDataBatchRequest(
                    instrument_ids=instrument_ids,
                )
            )
            return grpc_response.market_data_map
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e

    def get_greeks_batch(
        self, instrument_ids: list[str]
    ) -> dict[str, market_data_pb2.Greeks]:
        try:
            grpc_response = self.market_data_stub.GetGreeksBatch(
                market_data_service_pb2.GetGreeksBatchRequest(
                    instrument_ids=instrument_ids,
                )
            )
            return grpc_response.greeks_map
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e

    def get_synthetic_underlying_batch(
        self, instrument_ids: list[str]
    ) -> dict[str, market_data_pb2.SyntheticUnderlying]:
        try:
            grpc_response = self.market_data_stub.GetSyntheticUnderlyingBatch(
                market_data_service_pb2.GetSyntheticUnderlyingBatchRequest(
                    instrument_ids=instrument_ids,
                )
            )
            return grpc_response.synthetic_underlying_map
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise TroveGrpcClient.TroveServerUnavailableError("Trove server unavailable")
            else:
                raise e
