require "thetadata"

describe ThetaData::Streaming do
  def skip_unless_integration!
    skip "Set THETADATA_INTEGRATION=1 to run" unless ENV["THETADATA_INTEGRATION"]
  end

  def skip_unless_credentials!
    config = ThetaData.configuration
    skip "Set THETADATA_ACCOUNT_EMAIL and THETADATA_ACCOUNT_PASSWORD" if config.email.nil? || config.password.nil?
  end

  with "Endpoint" do
    it "connects to ThetaData FPSS server" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default

      endpoint.connect do |connection|
        expect(connection).to be_a(ThetaData::Streaming::Connection)
        expect(connection.closed?).to be == false
      end
    end
  end

  with "Client" do
    it "authenticates and receives metadata" do
      skip_unless_integration!
      skip_unless_credentials!

      endpoint = ThetaData::Streaming::Endpoint.default
      config = ThetaData.configuration

      ThetaData::Streaming::Client.open(endpoint) do |client|
        client.login(config.email, config.password)

        metadata_received = false
        5.times do
          event = client.read_event
          break if event.nil?

          if event[:type] == :metadata
            metadata_received = true
            break
          end
        end

        expect(metadata_received).to be == true
      end
    end

    it "subscribes to trade stream and receives data" do
      skip_unless_integration!
      skip_unless_credentials!

      endpoint = ThetaData::Streaming::Endpoint.default
      config = ThetaData.configuration

      ThetaData::Streaming::Client.open(endpoint) do |client|
        client.login(config.email, config.password)

        # Wait for metadata
        loop do
          event = client.read_event
          break if event.nil? || event[:type] == :metadata
        end

        contract = ThetaData::Streaming::Contract.index("SPX")
        request_id = client.subscribe_trade(contract)

        expect(request_id).to be == 1

        # Wait for subscription response
        response_received = false
        10.times do
          event = client.read_event
          break if event.nil?

          if event[:type] == :req_response && event[:request_id] == request_id
            response_received = true
            expect(event[:response]).to be == :subscribed
            break
          end
        end

        expect(response_received).to be == true
      end
    end

    it "receives trade ticks with FIT delta accumulation" do
      skip_unless_integration!
      skip_unless_credentials!
      skip "Trade data not always available on test server" if ThetaData.configuration.test?

      endpoint = ThetaData::Streaming::Endpoint.default
      config = ThetaData.configuration

      ThetaData::Streaming::Client.open(endpoint) do |client|
        client.login(config.email, config.password)

        # Wait for metadata
        loop do
          event = client.read_event
          break if event.nil? || event[:type] == :metadata
        end

        contract = ThetaData::Streaming::Contract.index("SPX")
        client.subscribe_trade(contract)

        # Collect some trade events
        trades = []
        50.times do
          event = client.read_event
          break if event.nil?

          if event[:type] == :trade
            trades << event
            break if trades.length >= 3
          end
        end

        expect(trades.length).to be >= 1

        if trades.length >= 1
          tick = trades.first[:tick]
          expect(tick).to be_a(ThetaData::Streaming::TradeTick)
          expect(tick.contract_id).to be_a(Integer)
        end
      end
    end
  end

  with "module-level client" do
    it "provides authenticated client via Streaming.client" do
      skip_unless_integration!
      skip_unless_credentials!

      begin
        client = ThetaData::Streaming.client

        expect(client).to be_a(ThetaData::Streaming::Client)
        expect(client.closed?).to be == false
      ensure
        ThetaData::Streaming.close
      end
    end
  end
end
