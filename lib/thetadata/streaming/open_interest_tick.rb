module ThetaData
  module Streaming
    OpenInterestTick = Data.define(
      :ms_of_day,
      :open_interest,
      :date,
    ) do
      def time
        hours = ms_of_day / 3600000
        minutes = (ms_of_day % 3600000) / 60000
        seconds = (ms_of_day % 60000) / 1000
        millis = ms_of_day % 1000

        format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
      end
    end
  end
end
