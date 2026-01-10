#!/usr/bin/env ruby

# Stream SPX prices from the test server (replay data)
#
# Usage:
#   bundle exec ruby examples/streaming_spx.rb

require_relative "../lib/thetadata"

ThetaData.configure do |config|
  config.env = :dev
end

puts "Theta Data index streaming: SPX"
puts "Environment: #{ThetaData.configuration.env}"
puts "Server: #{ThetaData.configuration.fpss_host}:#{ThetaData.configuration.fpss_port}"
puts
puts "Press Ctrl+C to stop"
puts "-" * 40

begin
  ThetaData::Streaming::Index.price_stream("SPX") do |event|
    tick = event[:tick]
    puts "SPX: #{'%.2f' % tick.price_decimal} at #{tick.time}"
  end
rescue Interrupt
  puts "\nStopped."
ensure
  ThetaData::Streaming.close
end
