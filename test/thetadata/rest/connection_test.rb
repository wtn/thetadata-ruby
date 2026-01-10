require "thetadata"
require "stringio"

describe ThetaData::REST::Connection do
  let(:email) { "test@example.com" }
  let(:password) { "password123" }
  let(:connection) { ThetaData::REST::Connection.new(email: email, password: password) }

  it "stores email" do
    expect(connection.email).to be == email
  end

  it "stores password" do
    expect(connection.password).to be == password
  end

  it "starts without session" do
    expect(connection.session).to be == nil
  end

  with "constants" do
    it "has MAX_RETRIES of 3" do
      expect(ThetaData::REST::Connection::MAX_RETRIES).to be == 3
    end

    it "has BASE_DELAY of 0.5" do
      expect(ThetaData::REST::Connection::BASE_DELAY).to be == 0.5
    end

    it "has MAX_DELAY of 8.0" do
      expect(ThetaData::REST::Connection::MAX_DELAY).to be == 8.0
    end

    it "defines TRANSIENT_ERRORS" do
      errors = ThetaData::REST::Connection::TRANSIENT_ERRORS
      expect(errors).to be(:include?, Protocol::GRPC::Unavailable)
      expect(errors).to be(:include?, Protocol::GRPC::DeadlineExceeded)
      expect(errors).to be(:include?, Protocol::GRPC::Internal)
      expect(errors).to be(:include?, EOFError)
      expect(errors).to be(:include?, IOError)
      expect(errors).to be(:include?, SocketError)
      expect(errors).to be(:include?, Errno::ECONNRESET)
      expect(errors).to be(:include?, Errno::ECONNREFUSED)
      expect(errors).to be(:include?, Errno::ETIMEDOUT)
    end
  end

  with "#authenticated?" do
    it "returns false when no session" do
      expect(connection.authenticated?).to be == false
    end

    it "returns false when session is expired" do
      expired_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
        created_at: Time.now - 4000,
      )
      connection.instance_variable_set(:@session, expired_session)

      expect(connection.authenticated?).to be == false
    end

    it "returns true when session is valid" do
      valid_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
      )
      connection.instance_variable_set(:@session, valid_session)

      expect(connection.authenticated?).to be == true
    end
  end

  with "#invalidate_session!" do
    it "clears the session" do
      valid_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
      )
      connection.instance_variable_set(:@session, valid_session)

      connection.send(:invalidate_session!)

      expect(connection.session).to be == nil
    end
  end

  with "#update_max_concurrency!" do
    it "sets max_concurrency based on subscription tier" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { stockSubscription: "STANDARD", optionsSubscription: "VALUE" },
      )
      connection.instance_variable_set(:@session, session)

      connection.send(:update_max_concurrency!)

      expect(ThetaData.configuration.max_concurrency).to be == 4
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "sets pro tier for highest subscription" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { indicesSubscription: 3 },
      )
      connection.instance_variable_set(:@session, session)

      connection.send(:update_max_concurrency!)

      expect(ThetaData.configuration.max_concurrency).to be == 8
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "defaults to 1 and warns for unknown tier" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { stockSubscription: "UNKNOWN" },
      )
      connection.instance_variable_set(:@session, session)

      original_stderr = $stderr
      $stderr = StringIO.new
      begin
        connection.send(:update_max_concurrency!)
        warning = $stderr.string
      ensure
        $stderr = original_stderr
      end

      expect(ThetaData.configuration.max_concurrency).to be == 1
      expect(warning).to be =~ /unknown subscription tier :unknown/
    ensure
      ThetaData.configuration.max_concurrency = 1
    end
  end

  with "#reset_grpc_client!" do
    it "closes and clears the grpc client" do
      mock_client = Object.new
      closed = false
      mock_client.define_singleton_method(:close) { closed = true }

      connection.instance_variable_set(:@grpc_client, mock_client)
      connection.send(:reset_grpc_client!)

      expect(closed).to be == true
      expect(connection.instance_variable_get(:@grpc_client)).to be == nil
    end

    it "handles nil grpc client" do
      connection.instance_variable_set(:@grpc_client, nil)
      connection.send(:reset_grpc_client!)

      expect(connection.instance_variable_get(:@grpc_client)).to be == nil
    end
  end

  with "#sleep_with_backoff" do
    it "calculates exponential delay with jitter" do
      delays = []
      connection.define_singleton_method(:sleep) { |d| delays << d }

      connection.send(:sleep_with_backoff, 2)
      connection.send(:sleep_with_backoff, 3)
      connection.send(:sleep_with_backoff, 4)

      expect(delays[0]).to be >= 0.5
      expect(delays[0]).to be < 0.75
      expect(delays[1]).to be >= 1.0
      expect(delays[1]).to be < 1.5
      expect(delays[2]).to be >= 2.0
      expect(delays[2]).to be < 3.0
    end

    it "caps delay at MAX_DELAY" do
      delays = []
      connection.define_singleton_method(:sleep) { |d| delays << d }

      connection.send(:sleep_with_backoff, 100)

      expect(delays[0]).to be <= 12.0
    end
  end

  with "#semaphore" do
    it "returns an Async::Semaphore" do
      semaphore = connection.send(:semaphore)
      expect(semaphore).to be_a(Async::Semaphore)
    end

    it "uses max_concurrency from configuration" do
      ThetaData.configuration.max_concurrency = 4
      semaphore = connection.send(:semaphore)
      expect(semaphore.limit).to be == 4
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "memoizes the semaphore" do
      semaphore1 = connection.send(:semaphore)
      semaphore2 = connection.send(:semaphore)
      expect(semaphore1).to be == semaphore2
    end
  end

  with "#query_info" do
    let(:session) { ThetaData::REST::Session.new(session_id: "test-session-id", user: {}) }

    it "includes session_id in auth_token" do
      connection.instance_variable_set(:@session, session)
      qi = connection.query_info

      expect(qi.auth_token.session_uuid).to be == "test-session-id"
    end

    it "sets client_type to a versioned ruby identifier" do
      connection.instance_variable_set(:@session, session)
      qi = connection.query_info

      expect(qi.client_type).to be == "thetadata-ruby-#{ThetaData::VERSION}"
    end

    # The Python client only sets auth_token; we add client_type so the server
    # can tell Ruby calls apart, but everything else stays at proto3 defaults.
    it "leaves terminal_git_commit, terminal_version, query_parameters at proto3 defaults" do
      connection.instance_variable_set(:@session, session)
      qi = connection.query_info

      expect(qi.terminal_git_commit).to be == ""
      expect(qi.terminal_version).to be == ""
      expect(qi.query_parameters.length).to be == 0
    end
  end

  with "#extract_value" do
    it "raises ServerError for unknown data type" do
      data_value = Object.new
      data_value.define_singleton_method(:data_type) { :unknown_type }

      expect do
        connection.send(:extract_value, data_value)
      end.to raise_exception(ThetaData::ServerError, message: be =~ /Unknown data type/)
    end
  end

  with "#decode_timestamp" do
    # Same epoch (ms) used across the cases so wall clocks differ only by zone.
    let(:epoch_ms) { 1700000000123 } # 2023-11-14T22:13:20.123Z

    it "returns Time in America/New_York when zone is NEW_YORK" do
      zdt = ::Endpoints::ZonedDateTime.new(epoch_ms: epoch_ms, zone: :NEW_YORK)
      t = connection.send(:decode_timestamp, zdt)
      expect(t).to be_a(Time)
      # Time#zone returns the TZInfo object when constructed via `in: tz`;
      # inspect via strftime, which renders the proper abbreviation.
      expect(t.strftime("%Z")).to be == "EST"  # standard time on 2023-11-14
      expect(t.utc_offset).to be == -5 * 3600
      expect(t.year).to be == 2023
      expect(t.month).to be == 11
      expect(t.day).to be == 14
      expect(t.hour).to be == 17
      expect(t.min).to be == 13
      expect(t.sec).to be == 20
      expect(t.subsec).to be == Rational(123, 1000)
    end

    it "returns Time in UTC when zone is UTC" do
      zdt = ::Endpoints::ZonedDateTime.new(epoch_ms: epoch_ms, zone: :UTC)
      t = connection.send(:decode_timestamp, zdt)
      expect(t).to be_a(Time)
      expect(t.zone).to be == "UTC"
      expect(t.utc_offset).to be == 0
      expect(t.hour).to be == 22
    end

    it "respects DST for NEW_YORK timestamps in summer" do
      summer_ms = 1721000000000 # 2024-07-14T22:13:20Z
      zdt = ::Endpoints::ZonedDateTime.new(epoch_ms: summer_ms, zone: :NEW_YORK)
      t = connection.send(:decode_timestamp, zdt)
      expect(t.strftime("%Z")).to be == "EDT"
      expect(t.utc_offset).to be == -4 * 3600
    end
  end

  with "#decode_price" do
    # Mirrors the Python client's PRICE_TYPE_FACTORS table; index 0 means "no data".
    it "returns nil for price_type 0 (no-data sentinel)" do
      expect(connection.send(:decode_price, 12345, 0)).to be == nil
    end

    it "returns nil when raw_value is nil" do
      expect(connection.send(:decode_price, nil, 10)).to be == nil
    end

    it "returns BigDecimal value at type 10 (1.0 factor)" do
      expect(connection.send(:decode_price, 12345, 10)).to be == BigDecimal("12345")
    end

    it "scales by 1e-2 at type 8" do
      expect(connection.send(:decode_price, 15450, 8)).to be == BigDecimal("154.50")
    end

    it "scales by 1e-4 at type 6" do
      expect(connection.send(:decode_price, 1234567, 6)).to be == BigDecimal("123.4567")
    end

    it "scales by 1e1 at type 11" do
      expect(connection.send(:decode_price, 5, 11)).to be == BigDecimal("50")
    end

    it "scales by 1e-9 at type 1 (smallest non-NaN factor)" do
      expect(connection.send(:decode_price, 1, 1)).to be == BigDecimal("0.000000001")
    end
  end

  with "#call error wrapping" do
    let(:connection) do
      conn = ThetaData::REST::Connection.new(email: "test@example.com", password: "pass")
      session = ThetaData::REST::Session.new(session_id: "test-id", user: {})
      conn.instance_variable_set(:@session, session)
      conn
    end

    let(:dummy_request) { Object.new }

    it "wraps Protocol::GRPC::NotFound as NotFoundError" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise Protocol::GRPC::NotFound.new("Not Found") }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::NotFoundError, message: be =~ /Not Found/)
    end

    it "wraps Permission Denied (status 7) as SubscriptionError" do
      error = Protocol::GRPC::Error.new(7, "Permission Denied")
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise error }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::SubscriptionError, message: be =~ /Permission Denied/)
    end

    it "wraps Protocol::GRPC::Unauthenticated as AuthenticationError after re-auth fails" do
      attempts = 0
      connection.define_singleton_method(:invoke_grpc) do |*, **|
        attempts += 1
        raise Protocol::GRPC::Unauthenticated.new("Unauthenticated")
      end
      connection.define_singleton_method(:authenticate!) do
        @session = ThetaData::REST::Session.new(session_id: "new-id", user: {})
        self
      end

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::AuthenticationError, message: be =~ /Unauthenticated/)
    end

    it "wraps Protocol::GRPC::InvalidArgument as ServerError" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise Protocol::GRPC::InvalidArgument.new("bad arg") }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::ServerError) do |error|
        expect(error.grpc_status).to be == 3
      end
    end

    it "wraps transient gRPC errors as ConnectionError after retries" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise Protocol::GRPC::Unavailable.new("unavailable") }
      connection.define_singleton_method(:sleep) { |_| }
      connection.define_singleton_method(:reset_grpc_client!) { }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::ConnectionError, message: be =~ /unavailable/)
    end

    it "wraps IO errors as ConnectionError after retries" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise EOFError.new("end of file") }
      connection.define_singleton_method(:sleep) { |_| }
      connection.define_singleton_method(:reset_grpc_client!) { }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::ConnectionError, message: be =~ /end of file/)
    end

    it "wraps other gRPC errors as ServerError with grpc_status" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise Protocol::GRPC::Cancelled.new("cancelled") }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::ServerError) do |error|
        expect(error.grpc_status).to be == 1
      end
    end

    it "preserves original error message" do
      connection.define_singleton_method(:invoke_grpc) { |*, **| raise Protocol::GRPC::NotFound.new("Symbol XYZ not found") }

      expect do
        connection.call(:SomeMethod, dummy_request)
      end.to raise_exception(ThetaData::NotFoundError, message: be =~ /Symbol XYZ not found/)
    end
  end

  with "#authenticate! timeout" do
    it "raises ThetaData::TimeoutError when auth_timeout elapses" do
      original_timeout = ThetaData.configuration.auth_timeout
      ThetaData.configuration.auth_timeout = 0.05

      slow_internet = Object.new
      slow_internet.define_singleton_method(:post) do |*_|
        sleep 5  # well past the configured timeout
      end
      connection.instance_variable_set(:@internet, slow_internet)

      expect do
        connection.authenticate!
      end.to raise_exception(ThetaData::TimeoutError, message: be =~ /Authentication timed out/)
    ensure
      ThetaData.configuration.auth_timeout = original_timeout
    end
  end

  with "integration" do
    def skip_unless_live
      unless ENV["THETADATA_EMAIL"]
        skip "Live API tests require THETADATA_EMAIL env var"
      end
    end

    it "can authenticate with real credentials" do
      skip_unless_live

      connection = ThetaData::REST::Connection.new(
        email: ENV["THETADATA_EMAIL"],
        password: ENV["THETADATA_PASSWORD"],
      )

      Async do
        connection.authenticate!
        expect(connection.authenticated?).to be == true
        expect(connection.session).to be_a(ThetaData::REST::Session)
      end.wait
    end
  end
end
