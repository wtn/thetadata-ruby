require "bigdecimal"

module ThetaData
  module Streaming
    OHLCTick = Data.define(
      :ms_of_day,
      :open,
      :high,
      :low,
      :close,
      :volume,
      :count,
      :price_type,
      :date,
    ) do
      def time
        hours = ms_of_day / 3600000
        minutes = (ms_of_day % 3600000) / 60000
        seconds = (ms_of_day % 60000) / 1000
        millis = ms_of_day % 1000

        format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
      end

      def open_decimal
        price_to_decimal(open)
      end

      def high_decimal
        price_to_decimal(high)
      end

      def low_decimal
        price_to_decimal(low)
      end

      def close_decimal
        price_to_decimal(close)
      end

      private

      def price_to_decimal(price)
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
