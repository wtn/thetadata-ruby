require "async"

module ThetaData
  module Streaming
    class << self
      def client
        @client ||= create_authenticated_client
      end

      def subscriptions
        client.subscriptions
      end

      def close
        @client&.close
        @client = nil
      end

      private

      def create_authenticated_client
        Sync do
          validate_credentials!

          config = ThetaData.configuration
          endpoint = Endpoint.default
          client = Client.open_async(endpoint)
          client.login(config.email, config.password)

          wait_for_metadata(client)

          client
        end
      end

      def validate_credentials!
        config = ThetaData.configuration
        if config.email.nil? || config.email.empty?
          raise AuthenticationError, "Missing email - set THETADATA_ACCOUNT_EMAIL or configure ThetaData.configuration.email"
        end
        if config.password.nil? || config.password.empty?
          raise AuthenticationError, "Missing password - set THETADATA_ACCOUNT_PASSWORD or configure ThetaData.configuration.password"
        end
      end

      def wait_for_metadata(client, timeout: ThetaData.configuration.auth_timeout)
        Async::Task.current.with_timeout(timeout) do
          loop do
            event = client.read_event
            if event.nil?
              raise AuthenticationError, "Connection closed before authentication completed"
            end

            case event[:type]
            when :metadata
              return event
            when :error, :disconnected
              raise AuthenticationError, "Failed to authenticate: #{event[:message] || event[:reason_code]}"
            end
          end
        end
      rescue Async::TimeoutError
        raise AuthenticationError, "Authentication timed out after #{timeout}s"
      end
    end
  end
end
