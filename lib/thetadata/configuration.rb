module ThetaData
  class Configuration
    attr_accessor :email, :password
    attr_accessor :auth_url, :terminal_key
    attr_accessor :mdds_host, :mdds_port
    attr_accessor :fpss_host, :fpss_port
    attr_accessor :session_ttl, :timeout
    attr_accessor :auth_timeout, :subscription_timeout, :read_timeout
    attr_accessor :max_concurrency
    attr_reader :env

    TIER_CONCURRENCY = {
      free: 1,
      value: 2,
      standard: 4,
      pro: 8,
    }.freeze

    SERVERS = {
      production: {
        mdds_host: "mdds-01.thetadata.us",
        mdds_port: 443,
        fpss_host: "nj-a.thetadata.us",
        fpss_port: 20000,
      },
      stage: {
        mdds_host: "mdds-01.thetadata.us",
        mdds_port: 443,
        fpss_host: "nj-a.thetadata.us",
        fpss_port: 20100,
      },
      dev: {
        mdds_host: "mdds-01.thetadata.us",
        mdds_port: 443,
        fpss_host: "nj-a.thetadata.us",
        fpss_port: 20200,
      },
    }.freeze

    def initialize
      @email = ENV["THETADATA_ACCOUNT_EMAIL"]
      @password = ENV["THETADATA_ACCOUNT_PASSWORD"]
      @auth_url = "https://nexus-api.thetadata.us/identity/terminal/auth_user"
      @terminal_key = "cf58ada4-4175-11f0-860f-1e2e95c79e64".freeze
      @session_ttl = 3600
      @timeout = 30
      @auth_timeout = 30
      @subscription_timeout = 30
      @read_timeout = 60
      @max_concurrency = TIER_CONCURRENCY[:free]

      self.env = ENV.fetch("THETADATA_ENV", "production").to_sym
    end

    def env=(value)
      @env = value.to_sym
      servers = SERVERS.fetch(@env, SERVERS[:production])
      @mdds_host = servers[:mdds_host]
      @mdds_port = servers[:mdds_port]
      @fpss_host = servers[:fpss_host]
      @fpss_port = servers[:fpss_port]
    end

    def production?
      @env == :production
    end

    def stage?
      @env == :stage
    end

    def dev?
      @env == :dev
    end
  end

  class << self
    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield(configuration)
    end
  end
end
