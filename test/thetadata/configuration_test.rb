require "thetadata"

describe ThetaData::Configuration do
  let(:config) { ThetaData::Configuration.new }

  it "has default auth_url" do
    expect(config.auth_url).to be == "https://nexus-api.thetadata.us/identity/terminal/auth_user"
  end

  it "has default terminal_key" do
    expect(config.terminal_key).to be == "cf58ada4-4175-11f0-860f-1e2e95c79e64"
  end

  it "has default mdds_host" do
    expect(config.mdds_host).to be == "mdds-01.thetadata.us"
  end

  it "has default mdds_port" do
    expect(config.mdds_port).to be == 443
  end

  it "has TERMINAL_GIT_COMMIT constant" do
    expect(ThetaData::TERMINAL_GIT_COMMIT).to be == "4a919e4f28d542cc672326d0ca709a457d1ef003"
  end

  it "has BOOTSTRAP_GIT_COMMIT constant" do
    expect(ThetaData::BOOTSTRAP_GIT_COMMIT).to be == "85346bbafda5bae4444038d0420dbfe65a4ad933"
  end

  it "has default session_ttl" do
    expect(config.session_ttl).to be == 3600
  end

  it "has default timeout" do
    expect(config.timeout).to be == 30
  end

  it "allows setting email" do
    config.email = "test@example.com"
    expect(config.email).to be == "test@example.com"
  end

  it "allows setting password" do
    config.password = "secret"
    expect(config.password).to be == "secret"
  end

  with "TIER_CONCURRENCY" do
    it "defines free tier as 1" do
      expect(ThetaData::Configuration::TIER_CONCURRENCY[:free]).to be == 1
    end

    it "defines value tier as 2" do
      expect(ThetaData::Configuration::TIER_CONCURRENCY[:value]).to be == 2
    end

    it "defines standard tier as 4" do
      expect(ThetaData::Configuration::TIER_CONCURRENCY[:standard]).to be == 4
    end

    it "defines pro tier as 8" do
      expect(ThetaData::Configuration::TIER_CONCURRENCY[:pro]).to be == 8
    end
  end

  with "max_concurrency" do
    it "defaults to free tier (1)" do
      expect(config.max_concurrency).to be == 1
    end

    it "can be set to a custom value" do
      config.max_concurrency = 4
      expect(config.max_concurrency).to be == 4
    end
  end
end

describe ThetaData do
  with ".configuration" do
    it "returns a Configuration instance" do
      expect(ThetaData.configuration).to be_a(ThetaData::Configuration)
    end

    it "returns the same instance" do
      expect(ThetaData.configuration).to be == ThetaData.configuration
    end
  end

  with ".configure" do
    it "yields the configuration" do
      ThetaData.configure do |config|
        expect(config).to be_a(ThetaData::Configuration)
      end
    end

    it "allows setting values via block" do
      ThetaData.configure do |config|
        config.email = "block@example.com"
      end
      expect(ThetaData.configuration.email).to be == "block@example.com"
    end
  end
end
