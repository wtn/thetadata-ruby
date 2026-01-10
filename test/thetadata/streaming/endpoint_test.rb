require "thetadata"
require "async"

describe ThetaData::Streaming::Endpoint do
  with "initialization" do
    it "accepts host and port" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      expect(endpoint.host).to be == "example.com"
      expect(endpoint.port).to be == 20000
    end

    it "uses default SSL context" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      expect(endpoint.ssl_context).to be_a(OpenSSL::SSL::SSLContext)
    end

    it "accepts custom SSL context" do
      custom_ctx = OpenSSL::SSL::SSLContext.new
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000, ssl_context: custom_ctx)

      expect(endpoint.ssl_context).to be == custom_ctx
    end

    it "accepts timeout option" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000, timeout: 30)

      expect(endpoint.timeout).to be == 30
    end

    it "has default timeout of 10 seconds" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      expect(endpoint.timeout).to be == 10
    end
  end

  with "#with" do
    it "returns new endpoint with merged options" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000, timeout: 10)
      new_endpoint = endpoint.with(timeout: 30)

      expect(new_endpoint.timeout).to be == 30
      expect(new_endpoint.host).to be == "example.com"
      expect(new_endpoint.port).to be == 20000
    end

    it "does not modify original endpoint" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000, timeout: 10)
      endpoint.with(timeout: 30)

      expect(endpoint.timeout).to be == 10
    end
  end

  with "#authority" do
    it "returns host:port string" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      expect(endpoint.authority).to be == "example.com:20000"
    end
  end

  with ".default" do
    it "creates endpoint from configuration" do
      config = ThetaData.configuration
      endpoint = ThetaData::Streaming::Endpoint.default

      expect(endpoint.host).to be == config.fpss_host
      expect(endpoint.port).to be == config.fpss_port
    end
  end

  with ".rotation" do
    it "returns 2 endpoints for production (one per host)" do
      ThetaData.configuration.env = :production
      endpoints = ThetaData::Streaming::Endpoint.rotation

      expect(endpoints.length).to be == 2

      hosts = endpoints.map(&:host)
      expect(hosts).to be(:include?, "nj-a.thetadata.us")
      expect(hosts).to be(:include?, "nj-b.thetadata.us")
      expect(endpoints.map(&:port).uniq).to be == [20000]
    ensure
      ThetaData.configuration.env = :production
    end

    it "returns single endpoint for non-production" do
      ThetaData.configuration.env = :dev
      endpoints = ThetaData::Streaming::Endpoint.rotation

      expect(endpoints.length).to be == 1
      expect(endpoints[0].host).to be == "nj-a.thetadata.us"
      expect(endpoints[0].port).to be == 20200
    ensure
      ThetaData.configuration.env = :production
    end
  end

  with ".connect_first" do
    it "returns endpoint and stream on success" do
      skip_unless_integration!

      Sync do
        endpoint, stream = ThetaData::Streaming::Endpoint.connect_first

        expect(endpoint).to be_a(ThetaData::Streaming::Endpoint)
        expect(stream).to respond_to(:read)
      ensure
        stream&.close
      end
    end

    it "raises last error when all endpoints fail" do
      ThetaData.configuration.env = :dev
      original_host = ThetaData.configuration.fpss_host
      ThetaData.configuration.fpss_host = "nonexistent.invalid.host.example"

      Sync do
        expect { ThetaData::Streaming::Endpoint.connect_first }.to raise_exception(SocketError)
      end
    ensure
      ThetaData.configuration.fpss_host = original_host
      ThetaData.configuration.env = :production
    end
  end

  with "#connect" do
    it "returns a Connection when successful" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default
      connection = endpoint.connect

      expect(connection).to be_a(ThetaData::Streaming::Connection)
      expect(connection.closed?).to be == false
    ensure
      connection&.close
    end

    it "yields connection to block and closes after" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default
      yielded_connection = nil

      endpoint.connect do |conn|
        yielded_connection = conn
        expect(conn.closed?).to be == false
      end

      expect(yielded_connection.closed?).to be == true
    end

    it "closes connection even if block raises" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default
      yielded_connection = nil

      begin
        endpoint.connect do |conn|
          yielded_connection = conn
          raise "test error"
        end
      rescue RuntimeError
        # expected
      end

      expect(yielded_connection.closed?).to be == true
    end

    it "raises error for invalid host" do
      endpoint = ThetaData::Streaming::Endpoint.new("nonexistent.invalid.host.example", 20000, timeout: 1)

      expect { endpoint.connect }.to raise_exception(SocketError)
    end

    it "raises error for connection refused" do
      # Port 1 is unlikely to be open
      endpoint = ThetaData::Streaming::Endpoint.new("127.0.0.1", 1, timeout: 1)

      expect { endpoint.connect }.to raise_exception(Errno::ECONNREFUSED)
    end
  end

  with "#with" do
    it "can change host" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)
      new_endpoint = endpoint.with(host: "other.com")

      expect(new_endpoint.host).to be == "other.com"
      expect(new_endpoint.port).to be == 20000
    end

    it "can change port" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)
      new_endpoint = endpoint.with(port: 30000)

      expect(new_endpoint.host).to be == "example.com"
      expect(new_endpoint.port).to be == 30000
    end

    it "can change ssl_context" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)
      new_ctx = OpenSSL::SSL::SSLContext.new
      new_endpoint = endpoint.with(ssl_context: new_ctx)

      expect(new_endpoint.ssl_context).to be == new_ctx
    end
  end

  with "#ssl_endpoint" do
    it "returns an IO::Endpoint::SSLEndpoint" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      expect(endpoint.ssl_endpoint).to be_a(IO::Endpoint::SSLEndpoint)
    end

    it "uses the configured host and port" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)
      ssl_ep = endpoint.ssl_endpoint

      expect(ssl_ep.to_s).to be(:include?, "example.com")
    end

    it "returns consistent ssl_endpoint" do
      endpoint = ThetaData::Streaming::Endpoint.new("example.com", 20000)

      # Two calls return equivalent endpoints
      ssl_ep1 = endpoint.ssl_endpoint
      ssl_ep2 = endpoint.ssl_endpoint

      expect(ssl_ep1.to_s).to be == ssl_ep2.to_s
    end
  end

  with "#connect_async" do
    it "returns a stream when called within Sync block" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default

      Sync do
        stream = endpoint.connect_async
        expect(stream).not.to be(:nil?)
        expect(stream).to respond_to(:read)
        expect(stream).to respond_to(:write)
      ensure
        stream&.close
      end
    end

    it "raises error for invalid host within Sync block" do
      endpoint = ThetaData::Streaming::Endpoint.new("nonexistent.invalid.host.example", 20000, timeout: 1)

      Sync do
        expect { endpoint.connect_async }.to raise_exception(SocketError)
      end
    end

    it "raises error for connection refused within Sync block" do
      endpoint = ThetaData::Streaming::Endpoint.new("127.0.0.1", 1, timeout: 1)

      Sync do
        expect { endpoint.connect_async }.to raise_exception(Errno::ECONNREFUSED)
      end
    end
  end
end

def skip_unless_integration!
  skip "Set THETADATA_INTEGRATION=1 to run" unless ENV["THETADATA_INTEGRATION"]
end
