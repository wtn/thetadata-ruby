require "thetadata"
require "stringio"

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

  with "env=" do
    def capture_warnings
      original = $stderr
      $stderr = StringIO.new
      yield
      $stderr.string
    ensure
      $stderr = original
    end

    it "switches server addresses for known envs" do
      config.env = :stage
      expect(config.env).to be == :stage
      expect(config.fpss_port).to be == 20100
    end

    it "warns and falls back to production for an unknown env" do
      output = capture_warnings { config.env = :stagings }
      expect(output).to be =~ /unknown env :stagings/
      expect(output).to be =~ /falling back to :production/
      expect(config.env).to be == :production
      expect(config.fpss_port).to be == 20000
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

  with "credentials file" do
    require "tempfile"

    def write_creds(email:, password: nil)
      f = Tempfile.new(["creds", ".txt"])
      f.puts email
      f.puts password if password
      f.close
      f.path
    end

    def with_env(vars)
      saved = vars.keys.to_h {|k| [k, ENV[k]] }
      vars.each {|k,v| ENV[k] = v }
      yield
    ensure
      saved.each {|k,v| ENV[k] = v }
    end

    it "reads email and password from THETADATA_CREDENTIALS_FILE when env vars are unset" do
      path = write_creds(email: "file@example.com", password: "filepass")
      with_env("THETADATA_CREDENTIALS_FILE" => path,
               "THETADATA_ACCOUNT_EMAIL" => nil,
               "THETADATA_ACCOUNT_PASSWORD" => nil) do
        c = ThetaData::Configuration.new
        expect(c.email).to be == "file@example.com"
        expect(c.password).to be == "filepass"
      end
    end

    it "lets THETADATA_CREDENTIALS_FILE take precedence over THETADATA_ACCOUNT_EMAIL / _PASSWORD" do
      path = write_creds(email: "file@example.com", password: "filepass")
      with_env("THETADATA_CREDENTIALS_FILE" => path,
               "THETADATA_ACCOUNT_EMAIL" => "env@example.com",
               "THETADATA_ACCOUNT_PASSWORD" => "envpass") do
        c = ThetaData::Configuration.new
        expect(c.email).to be == "file@example.com"
        expect(c.password).to be == "filepass"
      end
    end

    it "falls back to env vars for fields the file does not provide" do
      f = Tempfile.new(["creds", ".txt"])
      f.puts "file@example.com"
      f.close
      with_env("THETADATA_CREDENTIALS_FILE" => f.path,
               "THETADATA_ACCOUNT_EMAIL" => "env@example.com",
               "THETADATA_ACCOUNT_PASSWORD" => "envpass") do
        c = ThetaData::Configuration.new
        expect(c.email).to be == "file@example.com"
        expect(c.password).to be == "envpass"
      end
    end

    it "strips trailing whitespace from credentials" do
      f = Tempfile.new(["creds", ".txt"])
      f.write("\sspaced@example.com\s\n\spadded\s\n")
      f.close
      with_env("THETADATA_CREDENTIALS_FILE" => f.path,
               "THETADATA_ACCOUNT_EMAIL" => nil,
               "THETADATA_ACCOUNT_PASSWORD" => nil) do
        c = ThetaData::Configuration.new
        expect(c.email).to be == "spaced@example.com"
        expect(c.password).to be == "padded"
      end
    end

    it "raises if THETADATA_CREDENTIALS_FILE points to a missing file" do
      with_env("THETADATA_CREDENTIALS_FILE" => "/nonexistent/path/creds.txt",
               "THETADATA_ACCOUNT_EMAIL" => nil,
               "THETADATA_ACCOUNT_PASSWORD" => nil) do
        expect { ThetaData::Configuration.new }.to raise_exception(Errno::ENOENT)
      end
    end

    it "exposes load_credentials_file for explicit loading" do
      path = write_creds(email: "explicit@example.com", password: "explicitpass")
      with_env("THETADATA_CREDENTIALS_FILE" => nil,
               "THETADATA_ACCOUNT_EMAIL" => nil,
               "THETADATA_ACCOUNT_PASSWORD" => nil) do
        c = ThetaData::Configuration.new
        expect(c.email).to be_nil
        c.load_credentials_file(path)
        expect(c.email).to be == "explicit@example.com"
        expect(c.password).to be == "explicitpass"
      end
    end

    it "lets explicit assignment (e.g. via ThetaData.configure) override both the file and env vars" do
      path = write_creds(email: "file@example.com", password: "filepass")
      with_env("THETADATA_CREDENTIALS_FILE" => path,
               "THETADATA_ACCOUNT_EMAIL" => "env@example.com",
               "THETADATA_ACCOUNT_PASSWORD" => "envpass") do
        c = ThetaData::Configuration.new
        c.email = "explicit@example.com"
        c.password = "explicitpass"
        expect(c.email).to be == "explicit@example.com"
        expect(c.password).to be == "explicitpass"
      end
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
