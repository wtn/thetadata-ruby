require "bigdecimal"

module ThetaData
  module Streaming
    module PriceConversion
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
