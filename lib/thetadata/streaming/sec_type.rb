module ThetaData
  module Streaming
    module SecType
      IGNORE = -1
      STOCK = 0
      OPTION = 1
      INDEX = 2
      RATE = 3

      def self.name(code)
        constants.find { |c| const_get(c) == code }
      end
    end
  end
end
