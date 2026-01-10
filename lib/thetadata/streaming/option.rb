module ThetaData
  module Streaming
    module Option
      class << self
        include StreamHelpers

        def trade_stream(root:, expiration:, strike:, is_call:, &block)
          Sync do
            client = Streaming.client
            contract = Contract.option(
              root: root,
              expiration: expiration,
              strike: strike,
              is_call: is_call,
            )
            request_id = client.subscribe_trade(contract)

            wait_for_subscription(client, request_id)
            stream_events(client, :trade, &block)
          end
        end

        def quote_stream(root:, expiration:, strike:, is_call:, &block)
          Sync do
            client = Streaming.client
            contract = Contract.option(
              root: root,
              expiration: expiration,
              strike: strike,
              is_call: is_call,
            )
            request_id = client.subscribe_quote(contract)

            wait_for_subscription(client, request_id)
            stream_events(client, :quote, &block)
          end
        end

        def trade_streams(*contracts, &block)
          Sync do
            client = Streaming.client
            request_ids = {}

            contracts.flatten.each do |c|
              contract = Contract.option(
                root: c[:root],
                expiration: c[:expiration],
                strike: c[:strike],
                is_call: c[:is_call],
              )
              request_id = client.subscribe_trade(contract)
              request_ids[request_id] = c
            end

            wait_for_subscriptions(client, request_ids) { |id| request_ids[id][:root] }
            stream_events(client, :trade, &block)
          end
        end

        def quote_streams(*contracts, &block)
          Sync do
            client = Streaming.client
            request_ids = {}

            contracts.flatten.each do |c|
              contract = Contract.option(
                root: c[:root],
                expiration: c[:expiration],
                strike: c[:strike],
                is_call: c[:is_call],
              )
              request_id = client.subscribe_quote(contract)
              request_ids[request_id] = c
            end

            wait_for_subscriptions(client, request_ids) { |id| request_ids[id][:root] }
            stream_events(client, :quote, &block)
          end
        end
      end
    end
  end
end
