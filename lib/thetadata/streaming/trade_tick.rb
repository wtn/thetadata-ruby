require "bigdecimal"

module ThetaData
  module Streaming
    class TradeTick
      attr_reader :contract_id, :ms_of_day, :sequence,
                  :ext_con1, :ext_con2,
                  :price, :size, :price_type, :date

      CONTRACT_ID = 0
      MS_OF_DAY = 1
      SEQUENCE = 2
      EXT_CON1 = 3
      EXT_CON2 = 4
      PRICE = 5
      SIZE = 6
      PRICE_TYPE = 7
      DATE = 8

      RTH_OPEN_MS = 34_200_000
      RTH_CLOSE_MS = 57_600_000

      SELLER_CONDITION = 12

      def initialize
        @contract_id = 0
        @ms_of_day = 0
        @sequence = 0
        @ext_con1 = 0
        @ext_con2 = 0
        @price = 0
        @size = 0
        @price_type = 0
        @date = 0
      end

      def apply_changes(changes)
        changes.each_with_index do |value, idx|
          next if value.nil?

          case idx
          when CONTRACT_ID then @contract_id = value
          when MS_OF_DAY then @ms_of_day += value
          when SEQUENCE then @sequence += value
          when EXT_CON1 then @ext_con1 += value
          when EXT_CON2 then @ext_con2 += value
          when PRICE then @price += value
          when SIZE then @size += value
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
          sequence: @sequence,
          ext_con1: @ext_con1,
          ext_con2: @ext_con2,
          price: @price,
          size: @size,
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

      def price_decimal
        return BigDecimal("0") if @price_type == 0

        if @price_type == 10
          BigDecimal(@price)
        elsif @price_type > 10
          BigDecimal(@price) * (10 ** (@price_type - 10))
        else
          divisor = 10 ** (10 - @price_type)
          BigDecimal(@price) / divisor
        end
      end

      def regular_trading_hours?
        @ms_of_day >= RTH_OPEN_MS && @ms_of_day <= RTH_CLOSE_MS
      end

      def seller?
        @ext_con1 == SELLER_CONDITION
      end
    end
  end
end
