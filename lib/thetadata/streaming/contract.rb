module ThetaData
  module Streaming
    class Contract
      MAX_ROOT_LENGTH = 16
      MAX_UINT32 = 0xFFFFFFFF

      attr_reader :root, :sec_type, :expiration, :strike, :is_call

      def initialize(root:, sec_type:, expiration: nil, strike: nil, is_call: nil)
        validate_root!(root)
        validate_option_params!(expiration, strike, is_call) if sec_type == SecType::OPTION

        @root = root
        @sec_type = sec_type
        @expiration = expiration
        @strike = strike
        @is_call = is_call
      end

      def to_bytes
        if sec_type == SecType::OPTION
          encode_option
        else
          encode_simple
        end
      end

      def self.index(root)
        new(root: root, sec_type: SecType::INDEX)
      end

      def self.stock(root)
        new(root: root, sec_type: SecType::STOCK)
      end

      def self.option(root:, expiration:, strike:, is_call:)
        new(
          root: root,
          sec_type: SecType::OPTION,
          expiration: expiration,
          strike: strike,
          is_call: is_call,
        )
      end

      private

      def encode_simple
        total_length = 3 + root.bytesize

        [total_length, root.bytesize].pack("CC") << root << [sec_type].pack("C")
      end

      def encode_option
        total_length = 12 + root.bytesize
  
        [total_length, root.bytesize].pack("CC") \
          << root \
          << [sec_type, expiration, is_call ? 1 : 0, strike].pack("CNCN")
      end

      def validate_root!(root)
        raise ArgumentError, "Root symbol is required" if root.nil? || root.empty?
        raise ArgumentError, "Root too long: #{root.bytesize} bytes (max #{MAX_ROOT_LENGTH})" if root.bytesize > MAX_ROOT_LENGTH
      end

      def validate_option_params!(expiration, strike, is_call)
        raise ArgumentError, "Expiration is required for options" if expiration.nil?
        raise ArgumentError, "Strike is required for options" if strike.nil?
        raise ArgumentError, "is_call must be true or false" unless [true, false].include?(is_call)
        raise ArgumentError, "Strike must be positive" if strike <= 0
        raise ArgumentError, "Strike exceeds maximum value" if strike > MAX_UINT32
        raise ArgumentError, "Expiration exceeds maximum value" if expiration > MAX_UINT32
      end
    end
  end
end
