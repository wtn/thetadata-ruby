require "openssl"
require "async"
require "io/endpoint"
require "io/endpoint/ssl_endpoint"

module ThetaData
  module Streaming
    class Endpoint
      DEFAULT_TIMEOUT = 10

      attr_reader :host, :port, :ssl_context, :timeout

      def initialize(host, port, ssl_context: nil, timeout: DEFAULT_TIMEOUT)
        @host = host
        @port = port
        @ssl_context = ssl_context || OpenSSL::SSL::SSLContext.new
        @timeout = timeout
      end

      def self.default
        config = ThetaData.configuration
        new(config.fpss_host, config.fpss_port)
      end

      def with(**options)
        self.class.new(
          options.fetch(:host, @host),
          options.fetch(:port, @port),
          ssl_context: options.fetch(:ssl_context, @ssl_context),
          timeout: options.fetch(:timeout, @timeout),
        )
      end

      def authority
        "#{@host}:#{@port}"
      end

      def ssl_endpoint
        tcp_endpoint = IO::Endpoint.tcp(@host, @port)
        IO::Endpoint::SSLEndpoint.new(tcp_endpoint, ssl_context: @ssl_context)
      end

      def connect_async
        ssl_endpoint.connect
      end

      def connect(&block)
        if block_given?
          Sync do
            stream = connect_async
            connection = Connection.new(stream)
            begin
              yield connection
            ensure
              connection.close
            end
          end
        else
          Sync do
            stream = connect_async
            Connection.new(stream)
          end
        end
      end
    end
  end
end
