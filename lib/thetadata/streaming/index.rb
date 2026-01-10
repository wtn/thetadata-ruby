module ThetaData
  module Streaming
    module Index
      class << self
        include StreamHelpers

        def price_stream(symbol, &block)
          Sync do
            client = Streaming.client
            contract = Contract.index(symbol)
            request_id = client.subscribe_trade(contract)

            wait_for_subscription(client, request_id)
            stream_events(client, :trade, &block)
          end
        end

        def price_streams(*symbols, &block)
          Sync do
            client = Streaming.client
            request_ids = {}

            symbols.flatten.each do |symbol|
              contract = Contract.index(symbol)
              request_id = client.subscribe_trade(contract)
              request_ids[request_id] = symbol
            end

            wait_for_subscriptions(client, request_ids)
            stream_events(client, :trade, &block)
          end
        end
      end
    end
  end
end
