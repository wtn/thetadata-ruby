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
      include PriceConversion

      def time
        hours = ms_of_day / 3600000
        minutes = (ms_of_day % 3600000) / 60000
        seconds = (ms_of_day % 60000) / 1000
        millis = ms_of_day % 1000

        format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
      end

      def open_decimal
        price_to_decimal(open, price_type)
      end

      def high_decimal
        price_to_decimal(high, price_type)
      end

      def low_decimal
        price_to_decimal(low, price_type)
      end

      def close_decimal
        price_to_decimal(close, price_type)
      end
    end
  end
end
