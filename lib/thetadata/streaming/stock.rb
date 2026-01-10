module ThetaData
  module Streaming
    module Stock
      class << self
        include StreamHelpers

        def trade_stream(symbol, &block)
          Sync do
            client = Streaming.client
            contract = Contract.stock(symbol)
            request_id = client.subscribe_trade(contract)

            wait_for_subscription(client, request_id)
            stream_events(client, :trade, &block)
          end
        end

        def trade_streams(*symbols, &block)
          Sync do
            client = Streaming.client
            request_ids = {}

            symbols.flatten.each do |symbol|
              contract = Contract.stock(symbol)
              request_id = client.subscribe_trade(contract)
              request_ids[request_id] = symbol
            end

            wait_for_subscriptions(client, request_ids)
            stream_events(client, :trade, &block)
          end
        end

        def quote_stream(symbol, &block)
          Sync do
            client = Streaming.client
            contract = Contract.stock(symbol)
            request_id = client.subscribe_quote(contract)

            wait_for_subscription(client, request_id)
            stream_events(client, :quote, &block)
          end
        end

        def quote_streams(*symbols, &block)
          Sync do
            client = Streaming.client
            request_ids = {}

            symbols.flatten.each do |symbol|
              contract = Contract.stock(symbol)
              request_id = client.subscribe_quote(contract)
              request_ids[request_id] = symbol
            end

            wait_for_subscriptions(client, request_ids)
            stream_events(client, :quote, &block)
          end
        end
      end
    end
  end
end
