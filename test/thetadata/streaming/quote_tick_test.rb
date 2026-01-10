require "thetadata"

describe ThetaData::Streaming::QuoteTick do
  let(:quote) { ThetaData::Streaming::QuoteTick.new }

  with "field indices (verified against ThetaData Java source)" do
    it "has CONTRACT_ID = 0" do
      expect(ThetaData::Streaming::QuoteTick::CONTRACT_ID).to be == 0
    end

    it "has MS_OF_DAY = 1" do
      expect(ThetaData::Streaming::QuoteTick::MS_OF_DAY).to be == 1
    end

    it "has BID_SIZE = 2" do
      expect(ThetaData::Streaming::QuoteTick::BID_SIZE).to be == 2
    end

    it "has BID_EXCHANGE = 3" do
      expect(ThetaData::Streaming::QuoteTick::BID_EXCHANGE).to be == 3
    end

    it "has BID = 4" do
      expect(ThetaData::Streaming::QuoteTick::BID).to be == 4
    end

    it "has BID_CONDITION = 5" do
      expect(ThetaData::Streaming::QuoteTick::BID_CONDITION).to be == 5
    end

    it "has ASK_SIZE = 6" do
      expect(ThetaData::Streaming::QuoteTick::ASK_SIZE).to be == 6
    end

    it "has ASK_EXCHANGE = 7" do
      expect(ThetaData::Streaming::QuoteTick::ASK_EXCHANGE).to be == 7
    end

    it "has ASK = 8" do
      expect(ThetaData::Streaming::QuoteTick::ASK).to be == 8
    end

    it "has ASK_CONDITION = 9" do
      expect(ThetaData::Streaming::QuoteTick::ASK_CONDITION).to be == 9
    end

    it "has PRICE_TYPE = 10 (shared between bid and ask)" do
      expect(ThetaData::Streaming::QuoteTick::PRICE_TYPE).to be == 10
    end

    it "has DATE = 11" do
      expect(ThetaData::Streaming::QuoteTick::DATE).to be == 11
    end
  end

  with "initial state" do
    it "starts with all zeros" do
      expect(quote.contract_id).to be == 0
      expect(quote.ms_of_day).to be == 0
      expect(quote.bid_size).to be == 0
      expect(quote.bid_exchange).to be == 0
      expect(quote.bid).to be == 0
      expect(quote.bid_condition).to be == 0
      expect(quote.ask_size).to be == 0
      expect(quote.ask_exchange).to be == 0
      expect(quote.ask).to be == 0
      expect(quote.ask_condition).to be == 0
      expect(quote.price_type).to be == 0
      expect(quote.date).to be == 0
    end
  end

  with "#apply_changes" do
    it "sets contract_id as ABSOLUTE value (not delta)" do
      quote.apply_changes([1802875, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.contract_id).to be == 1802875

      # Same contract_id in next message should NOT accumulate
      quote.apply_changes([1802875, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.contract_id).to be == 1802875
    end

    it "accumulates ms_of_day as delta" do
      quote.apply_changes([100, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.ms_of_day).to be == 1000

      quote.apply_changes([100, 500, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.ms_of_day).to be == 1500
    end

    it "accumulates bid as delta" do
      # Fields: contract_id, ms_of_day, bid_size, bid_exg, bid, bid_cond, ask_size, ask_exg, ask, ask_cond, price_type, date
      quote.apply_changes([100, 0, 0, 0, 15000, 0, 0, 0, 0, 0, 8, 0])
      expect(quote.bid).to be == 15000

      quote.apply_changes([100, 0, 0, 0, 50, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.bid).to be == 15050
    end

    it "accumulates ask as delta" do
      quote.apply_changes([100, 0, 0, 0, 0, 0, 0, 0, 15100, 0, 8, 0])
      expect(quote.ask).to be == 15100

      quote.apply_changes([100, 0, 0, 0, 0, 0, 0, 0, -50, 0, 0, 0])
      expect(quote.ask).to be == 15050
    end

    it "accumulates bid_size and ask_size as deltas" do
      quote.apply_changes([100, 0, 100, 0, 0, 0, 200, 0, 0, 0, 0, 0])
      expect(quote.bid_size).to be == 100
      expect(quote.ask_size).to be == 200

      quote.apply_changes([100, 0, -50, 0, 0, 0, 50, 0, 0, 0, 0, 0])
      expect(quote.bid_size).to be == 50
      expect(quote.ask_size).to be == 250
    end

    it "accumulates bid_condition and ask_condition as deltas" do
      quote.apply_changes([100, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0])
      expect(quote.bid_condition).to be == 1
      expect(quote.ask_condition).to be == 2
    end

    it "accumulates shared price_type as delta" do
      quote.apply_changes([100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0])
      expect(quote.price_type).to be == 8

      quote.apply_changes([100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.price_type).to be == 8  # No change
    end

    it "returns self for chaining" do
      result = quote.apply_changes([100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(result).to be == quote
    end
  end

  with "#time" do
    it "formats ms_of_day as HH:MM:SS.mmm" do
      quote.apply_changes([0, 53790000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      expect(quote.time).to be == "14:56:30.000"
    end
  end

  with "#bid_decimal" do
    it "returns 0 when price_type is 0" do
      expect(quote.bid_decimal).to be == BigDecimal("0")
    end

    it "uses shared price_type for conversion" do
      # bid=15000, price_type=8 -> 150.00
      quote.apply_changes([0, 0, 0, 0, 15000, 0, 0, 0, 0, 0, 8, 0])
      expect(quote.bid_decimal).to be == BigDecimal("150.00")
    end
  end

  with "#ask_decimal" do
    it "returns 0 when price_type is 0" do
      expect(quote.ask_decimal).to be == BigDecimal("0")
    end

    it "uses shared price_type for conversion" do
      # ask=15100, price_type=8 -> 151.00
      quote.apply_changes([0, 0, 0, 0, 0, 0, 0, 0, 15100, 0, 8, 0])
      expect(quote.ask_decimal).to be == BigDecimal("151.00")
    end
  end

  with "#mid_decimal" do
    it "returns average of bid and ask using shared price_type" do
      quote.apply_changes([0, 0, 0, 0, 15000, 0, 0, 0, 15100, 0, 8, 0])
      expect(quote.mid_decimal).to be == BigDecimal("150.50")
    end
  end

  with "#spread_decimal" do
    it "returns ask minus bid" do
      quote.apply_changes([0, 0, 0, 0, 15000, 0, 0, 0, 15100, 0, 8, 0])
      expect(quote.spread_decimal).to be == BigDecimal("1.00")
    end
  end

  with "#to_h" do
    it "returns hash of all fields including conditions" do
      quote.apply_changes([1802875, 53790000, 100, 1, 15000, 5, 200, 2, 15100, 6, 8, 20250110])
      h = quote.to_h

      expect(h[:contract_id]).to be == 1802875
      expect(h[:ms_of_day]).to be == 53790000
      expect(h[:bid_size]).to be == 100
      expect(h[:bid_exchange]).to be == 1
      expect(h[:bid]).to be == 15000
      expect(h[:bid_condition]).to be == 5
      expect(h[:ask_size]).to be == 200
      expect(h[:ask_exchange]).to be == 2
      expect(h[:ask]).to be == 15100
      expect(h[:ask_condition]).to be == 6
      expect(h[:price_type]).to be == 8
      expect(h[:date]).to be == 20250110
    end
  end

  with "#clone" do
    it "creates independent copy" do
      quote.apply_changes([1802875, 53790000, 100, 1, 15000, 5, 200, 2, 15100, 6, 8, 20250110])
      cloned = quote.clone

      expect(cloned.contract_id).to be == quote.contract_id
      expect(cloned.bid).to be == quote.bid
      expect(cloned.bid_condition).to be == quote.bid_condition

      # Modify original
      quote.apply_changes([1802875, 1000, 0, 0, 50, 0, 0, 0, 50, 0, 0, 0])

      # Clone should be unchanged
      expect(cloned.bid).to be == 15000
      expect(cloned.ask).to be == 15100
    end
  end

  with "realistic scenario" do
    it "accumulates deltas correctly over multiple quotes" do
      # First quote: establish baseline
      quote.apply_changes([1802875, 53790000, 100, 1, 15000, 0, 200, 2, 15100, 0, 8, 20250110])

      expect(quote.contract_id).to be == 1802875
      expect(quote.bid_decimal).to be == BigDecimal("150.00")
      expect(quote.ask_decimal).to be == BigDecimal("151.00")
      expect(quote.spread_decimal).to be == BigDecimal("1.00")

      # Second quote: bid improves
      quote.apply_changes([1802875, 100, 50, 0, 25, 0, 0, 0, 0, 0, 0, 0])

      expect(quote.contract_id).to be == 1802875
      expect(quote.bid).to be == 15025
      expect(quote.bid_decimal).to be == BigDecimal("150.25")
      expect(quote.spread_decimal).to be == BigDecimal("0.75")

      # Third quote: ask comes down
      quote.apply_changes([1802875, 100, 0, 0, 0, 0, 0, 0, -50, 0, 0, 0])

      expect(quote.ask).to be == 15050
      expect(quote.ask_decimal).to be == BigDecimal("150.50")
      expect(quote.spread_decimal).to be == BigDecimal("0.25")
    end
  end
end
