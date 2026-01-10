require "bigdecimal"

module ThetaData
  module Streaming
    class QuoteTick
      attr_reader :contract_id, :ms_of_day, :bid_size, :bid_exchange, :bid,
                  :bid_condition, :ask_size, :ask_exchange, :ask, :ask_condition,
                  :price_type, :date

      CONTRACT_ID = 0
      MS_OF_DAY = 1
      BID_SIZE = 2
      BID_EXCHANGE = 3
      BID = 4
      BID_CONDITION = 5
      ASK_SIZE = 6
      ASK_EXCHANGE = 7
      ASK = 8
      ASK_CONDITION = 9
      PRICE_TYPE = 10
      DATE = 11

      def initialize
        @contract_id = 0
        @ms_of_day = 0
        @bid_size = 0
        @bid_exchange = 0
        @bid = 0
        @bid_condition = 0
        @ask_size = 0
        @ask_exchange = 0
        @ask = 0
        @ask_condition = 0
        @price_type = 0
        @date = 0
      end

      def apply_changes(changes)
        changes.each_with_index do |value, idx|
          next if value.nil?

          case idx
          when CONTRACT_ID then @contract_id = value
          when MS_OF_DAY then @ms_of_day += value
          when BID_SIZE then @bid_size += value
          when BID_EXCHANGE then @bid_exchange += value
          when BID then @bid += value
          when BID_CONDITION then @bid_condition += value
          when ASK_SIZE then @ask_size += value
          when ASK_EXCHANGE then @ask_exchange += value
          when ASK then @ask += value
          when ASK_CONDITION then @ask_condition += value
          when PRICE_TYPE then @price_type += value
          when DATE then @date += value
          end
        end

        self
      end

      def to_h
        {
          contract_id: @contract_id,
          ms_of_day: @ms_of_day,
          bid_size: @bid_size,
          bid_exchange: @bid_exchange,
          bid: @bid,
          bid_condition: @bid_condition,
          ask_size: @ask_size,
          ask_exchange: @ask_exchange,
          ask: @ask,
          ask_condition: @ask_condition,
          price_type: @price_type,
          date: @date,
        }
      end

      def time
        hours = @ms_of_day / 3600000
        minutes = (@ms_of_day % 3600000) / 60000
        seconds = (@ms_of_day % 60000) / 1000
        millis = @ms_of_day % 1000

        format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
      end

      def bid_decimal
        price_to_decimal(@bid, @price_type)
      end

      def ask_decimal
        price_to_decimal(@ask, @price_type)
      end

      def mid_decimal
        (bid_decimal + ask_decimal) / 2
      end

      def spread_decimal
        ask_decimal - bid_decimal
      end

      private

      def price_to_decimal(price, price_type)
        return BigDecimal("0") if price_type == 0

        if price_type == 10
          BigDecimal(price)
        elsif price_type > 10
          BigDecimal(price) * (10 ** (price_type - 10))
        else
          divisor = 10 ** (10 - price_type)
          BigDecimal(price) / divisor
        end
      end
    end
  end
end
