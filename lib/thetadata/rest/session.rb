module ThetaData
  module REST
    class Session
      TIER_ORDER = %w[FREE VALUE STANDARD PRO].freeze
      TIER_FROM_INT = {
        0 => "FREE",
        1 => "VALUE",
        2 => "STANDARD",
        3 => "PRO",
      }.freeze

      attr_reader :session_id, :user, :created_at

      def initialize(session_id:, user:, created_at: nil)
        @session_id = session_id
        @user = user
        @created_at = created_at || Time.now
      end

      def valid?
        !expired?
      end

      def expired?
        Time.now - @created_at >= ThetaData.configuration.session_ttl
      end

      def subscription_tier
        return "FREE" if @user.nil?

        tiers = [
          @user[:stockSubscription],
          @user[:optionsSubscription],
          @user[:indicesSubscription],
        ].compact.map { |t| normalize_tier(t) }

        tiers.max_by { |t| TIER_ORDER.index(t) || -1 } || "FREE"
      end

      private

      def normalize_tier(tier)
        case tier
        when Integer
          TIER_FROM_INT[tier] || "FREE"
        when String
          tier = "PRO" if tier == "PROFESSIONAL" || tier == "FULL"
          tier
        else
          "FREE"
        end
      end
    end
  end
end
