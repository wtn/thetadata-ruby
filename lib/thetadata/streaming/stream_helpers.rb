require "async"

module ThetaData
  module Streaming
    module StreamHelpers
      def wait_for_subscription(client, request_id, timeout: ThetaData.configuration.subscription_timeout)
        Async::Task.current.with_timeout(timeout) do
          loop do
            event = client.read_event
            if event.nil?
              raise SubscriptionError, "Connection closed while waiting for subscription"
            end

            if event[:type] == :req_response && event[:request_id] == request_id
              unless event[:response] == :subscribed
                raise SubscriptionError, "Failed to subscribe: #{event[:response]}"
              end
              return
            end
          end
        end
      rescue Async::TimeoutError
        raise SubscriptionError, "Subscription timed out after #{timeout}s"
      end

      def wait_for_subscriptions(client, request_ids, timeout: ThetaData.configuration.subscription_timeout, &error_formatter)
        error_formatter ||= ->(id) { request_ids[id].to_s }

        Async::Task.current.with_timeout(timeout) do
          pending = request_ids.keys.dup
          loop do
            break if pending.empty?

            event = client.read_event
            if event.nil?
              raise SubscriptionError, "Connection closed while waiting for subscriptions"
            end

            if event[:type] == :req_response
              pending.delete(event[:request_id])
              unless event[:response] == :subscribed
                identifier = error_formatter.call(event[:request_id])
                raise SubscriptionError, "Failed to subscribe to #{identifier}: #{event[:response]}"
              end
            end
          end
        end
      rescue Async::TimeoutError
        raise SubscriptionError, "Subscriptions timed out after #{timeout}s"
      end

      def stream_events(client, event_type, &block)
        return async_event_enumerator(client, event_type) unless block_given?

        loop do
          event = client.read_event
          break if event.nil?
          next unless event[:type] == event_type
          yield event
        end
      rescue LocalJumpError
        # User called break in their block
      end

      private

      def async_event_enumerator(client, event_type)
        Enumerator.new do |yielder|
          loop do
            event = client.read_event
            break if event.nil?
            next unless event[:type] == event_type
            yielder << event
          end
        end
      end
    end
  end
end
