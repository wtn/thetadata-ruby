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
        # SSLContext.new defaults to VERIFY_NONE in Ruby. This is fine here:
        # ThetaData's cert is self-signed and expired (since Jan 2024).
        # TLS still provides encryption; only chain validation is skipped.
        @ssl_context = ssl_context || OpenSSL::SSL::SSLContext.new
        @timeout = timeout
      end

      PRODUCTION_HOSTS = ["nj-a.thetadata.us", "nj-b.thetadata.us"].freeze
      PRODUCTION_PORTS = [20000].freeze

      def self.default
        config = ThetaData.configuration
        new(config.fpss_host, config.fpss_port)
      end

      # Build ordered endpoint list for production: random host first, then ports.
      # Falls back to configured host/port for non-production envs.
      def self.rotation
        config = ThetaData.configuration
        unless config.production?
          return [new(config.fpss_host, config.fpss_port)]
        end

        hosts = PRODUCTION_HOSTS.shuffle
        hosts.flat_map do |host|
          PRODUCTION_PORTS.map {|port| new(host, port) }
        end
      end

      # Try endpoints in order, return the first that connects.
      def self.connect_first
        endpoints = rotation
        last_error = nil

        endpoints.each do |endpoint|
          stream = endpoint.connect_async
          return [endpoint, stream]
        rescue => e
          last_error = e
        end

        raise last_error
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
