require "bigdecimal"

module ThetaData
  module Streaming
    EODTick = Data.define(
      :ms_of_day,
      :ms_of_day2,
      :open,
      :high,
      :low,
      :close,
      :volume,
      :count,
      :bid_size,
      :bid_exchange,
      :bid,
      :bid_condition,
      :ask_size,
      :ask_exchange,
      :ask,
      :ask_condition,
      :price_type,
      :date,
    ) do
      include PriceConversion

      def time
        format_time(ms_of_day)
      end

      def last_trade_time
        format_time(ms_of_day2)
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

      def bid_decimal
        price_to_decimal(bid, price_type)
      end

      def ask_decimal
        price_to_decimal(ask, price_type)
      end

      def mid_decimal
        (bid_decimal + ask_decimal) / 2
      end

      def spread_decimal
        ask_decimal - bid_decimal
      end

      private

      def format_time(ms)
        hours = ms / 3600000
        minutes = (ms % 3600000) / 60000
        seconds = (ms % 60000) / 1000
        millis = ms % 1000

        format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
      end
    end
  end
end
