require "thetadata"

describe ThetaData::Streaming::TradeTick do
  let(:tick) { ThetaData::Streaming::TradeTick.new }

  # Helper to create changes array matching server format:
  # [contract_id, ms_of_day, sequence, ext_con1, ext_con2, price, size, price_type, date]
  def changes(
    contract_id: 0, ms_of_day: 0, sequence: 0,
    ext_con1: 0, ext_con2: 0,
    price: 0, size: 0, price_type: 0, date: 0
  )
    [contract_id, ms_of_day, sequence, ext_con1, ext_con2, price, size, price_type, date]
  end

  with "field indices" do
    it "has CONTRACT_ID = 0" do
      expect(ThetaData::Streaming::TradeTick::CONTRACT_ID).to be == 0
    end

    it "has MS_OF_DAY = 1" do
      expect(ThetaData::Streaming::TradeTick::MS_OF_DAY).to be == 1
    end

    it "has SEQUENCE = 2" do
      expect(ThetaData::Streaming::TradeTick::SEQUENCE).to be == 2
    end

    it "has EXT_CON1 = 3" do
      expect(ThetaData::Streaming::TradeTick::EXT_CON1).to be == 3
    end

    it "has EXT_CON2 = 4" do
      expect(ThetaData::Streaming::TradeTick::EXT_CON2).to be == 4
    end

    it "has PRICE = 5" do
      expect(ThetaData::Streaming::TradeTick::PRICE).to be == 5
    end

    it "has SIZE = 6" do
      expect(ThetaData::Streaming::TradeTick::SIZE).to be == 6
    end

    it "has PRICE_TYPE = 7" do
      expect(ThetaData::Streaming::TradeTick::PRICE_TYPE).to be == 7
    end

    it "has DATE = 8" do
      expect(ThetaData::Streaming::TradeTick::DATE).to be == 8
    end
  end

  with "initial state" do
    it "starts with all zeros" do
      expect(tick.contract_id).to be == 0
      expect(tick.ms_of_day).to be == 0
      expect(tick.sequence).to be == 0
      expect(tick.ext_con1).to be == 0
      expect(tick.ext_con2).to be == 0
      expect(tick.price).to be == 0
      expect(tick.size).to be == 0
      expect(tick.price_type).to be == 0
      expect(tick.date).to be == 0
    end
  end

  with "#apply_changes" do
    it "sets contract_id as ABSOLUTE value (not delta)" do
      tick.apply_changes(changes(contract_id: 1802875))
      expect(tick.contract_id).to be == 1802875

      tick.apply_changes(changes(contract_id: 1802875))
      expect(tick.contract_id).to be == 1802875
    end

    it "accumulates ms_of_day as delta" do
      tick.apply_changes(changes(contract_id: 100, ms_of_day: 1000))
      expect(tick.ms_of_day).to be == 1000

      tick.apply_changes(changes(contract_id: 100, ms_of_day: 500))
      expect(tick.ms_of_day).to be == 1500
    end

    it "accumulates sequence as delta" do
      tick.apply_changes(changes(contract_id: 100, sequence: 1))
      expect(tick.sequence).to be == 1

      tick.apply_changes(changes(contract_id: 100, sequence: 1))
      expect(tick.sequence).to be == 2
    end

    it "accumulates ext_con fields as deltas" do
      tick.apply_changes(changes(contract_id: 100, ext_con1: 5, ext_con2: 10))
      expect(tick.ext_con1).to be == 5
      expect(tick.ext_con2).to be == 10
    end

    it "accumulates size as delta" do
      tick.apply_changes(changes(contract_id: 100, size: 100))
      expect(tick.size).to be == 100

      tick.apply_changes(changes(contract_id: 100, size: -50))
      expect(tick.size).to be == 50
    end

    it "accumulates price as delta" do
      tick.apply_changes(changes(contract_id: 100, price: 550564, price_type: 8))
      expect(tick.price).to be == 550564

      tick.apply_changes(changes(contract_id: 100, price: 8))
      expect(tick.price).to be == 550572
    end

    it "accumulates price_type as delta" do
      tick.apply_changes(changes(contract_id: 100, price_type: 8))
      expect(tick.price_type).to be == 8

      tick.apply_changes(changes(contract_id: 100))
      expect(tick.price_type).to be == 8
    end

    it "accumulates date as delta" do
      tick.apply_changes(changes(contract_id: 100, date: 20250110))
      expect(tick.date).to be == 20250110

      tick.apply_changes(changes(contract_id: 100, date: 1))
      expect(tick.date).to be == 20250111
    end

    it "returns self for chaining" do
      result = tick.apply_changes(changes(contract_id: 100))
      expect(result).to be == tick
    end

    it "handles partial changes array" do
      tick.apply_changes([100, 1000])
      expect(tick.contract_id).to be == 100
      expect(tick.ms_of_day).to be == 1000
      expect(tick.price).to be == 0
    end
  end

  with "#time" do
    it "formats ms_of_day as HH:MM:SS.mmm" do
      tick.apply_changes(changes(ms_of_day: 53790000))
      expect(tick.time).to be == "14:56:30.000"
    end

    it "formats with milliseconds" do
      tick.apply_changes(changes(ms_of_day: 34200123))
      expect(tick.time).to be == "09:30:00.123"
    end

    it "formats zero as 00:00:00.000" do
      expect(tick.time).to be == "00:00:00.000"
    end
  end

  with "#price_decimal" do
    it "returns 0 when price_type is 0" do
      expect(tick.price_decimal).to be == BigDecimal("0")
    end

    it "divides by 100 for price_type 8 (2 decimals)" do
      tick.apply_changes(changes(price: 550564, price_type: 8))
      expect(tick.price_decimal).to be == BigDecimal("5505.64")
    end

    it "divides by 1000 for price_type 7 (3 decimals)" do
      tick.apply_changes(changes(price: 123456, price_type: 7))
      expect(tick.price_decimal).to be == BigDecimal("123.456")
    end

    it "divides by 10000 for price_type 6 (4 decimals)" do
      tick.apply_changes(changes(price: 123456, price_type: 6))
      expect(tick.price_decimal).to be == BigDecimal("12.3456")
    end

    it "returns whole number for price_type 10" do
      tick.apply_changes(changes(price: 5500, price_type: 10))
      expect(tick.price_decimal).to be == BigDecimal("5500")
    end

    it "multiplies by 10 for price_type 11" do
      tick.apply_changes(changes(price: 550, price_type: 11))
      expect(tick.price_decimal).to be == BigDecimal("5500")
    end
  end

  with "#to_h" do
    it "returns hash of all fields" do
      tick.apply_changes(changes(
        contract_id: 1802875, ms_of_day: 53790000, sequence: 1,
        ext_con1: 12, ext_con2: 0,
        price: 550564, size: 100, price_type: 8, date: 20250110,
      ))
      h = tick.to_h

      expect(h[:contract_id]).to be == 1802875
      expect(h[:ms_of_day]).to be == 53790000
      expect(h[:sequence]).to be == 1
      expect(h[:ext_con1]).to be == 12
      expect(h[:size]).to be == 100
      expect(h[:price]).to be == 550564
      expect(h[:price_type]).to be == 8
      expect(h[:date]).to be == 20250110
    end
  end

  with "#clone" do
    it "creates independent copy" do
      tick.apply_changes(changes(
        contract_id: 1802875, ms_of_day: 53790000, price: 550564, price_type: 8,
      ))
      cloned = tick.clone

      expect(cloned.contract_id).to be == tick.contract_id
      expect(cloned.price).to be == tick.price

      tick.apply_changes(changes(contract_id: 1802875, ms_of_day: 1000, price: 100))

      expect(cloned.ms_of_day).to be == 53790000
      expect(cloned.price).to be == 550564
    end
  end

  with "#seller?" do
    it "returns true when ext_con1 is 12" do
      tick.apply_changes(changes(ext_con1: 12))
      expect(tick.seller?).to be == true
    end

    it "returns false when ext_con1 is not 12" do
      tick.apply_changes(changes(ext_con1: 11))
      expect(tick.seller?).to be == false
    end
  end

  with "#regular_trading_hours?" do
    it "returns true during market hours (9:30-16:00 ET)" do
      tick.apply_changes(changes(ms_of_day: 34200000))
      expect(tick.regular_trading_hours?).to be == true

      tick.apply_changes(changes(ms_of_day: 23400000))
      expect(tick.regular_trading_hours?).to be == true
    end

    it "returns false before market open" do
      tick.apply_changes(changes(ms_of_day: 34199999))
      expect(tick.regular_trading_hours?).to be == false
    end

    it "returns false after market close" do
      tick.apply_changes(changes(ms_of_day: 57600001))
      expect(tick.regular_trading_hours?).to be == false
    end
  end

  with "real server data" do
    it "parses index tick from test server" do
      raw_changes = [1802875, 36340000, 0, 0, 0, 553020, 5, 8, 20250428]

      tick.apply_changes(raw_changes)

      expect(tick.contract_id).to be == 1802875
      expect(tick.ms_of_day).to be == 36340000
      expect(tick.price).to be == 553020
      expect(tick.price_type).to be == 8
      expect(tick.date).to be == 20250428
      expect(tick.price_decimal).to be == BigDecimal("5530.20")
    end

    it "accumulates deltas from server data" do
      tick.apply_changes([1802875, 36340000, 0, 0, 0, 553020, 5, 8, 20250428])
      tick.apply_changes([1802875, 1000, 0, 0, 0, -1, 0, 0, 0])

      expect(tick.ms_of_day).to be == 36341000
      expect(tick.price).to be == 553019
      expect(tick.price_decimal).to be == BigDecimal("5530.19")
    end
  end

  with "realistic scenario" do
    it "accumulates deltas correctly over multiple ticks" do
      tick.apply_changes(changes(
        contract_id: 1802875, ms_of_day: 53790000, sequence: 1,
        price: 550564, size: 100, price_type: 8, date: 20250110,
      ))

      expect(tick.contract_id).to be == 1802875
      expect(tick.price_decimal).to be == BigDecimal("5505.64")

      tick.apply_changes(changes(
        contract_id: 1802875, ms_of_day: 1000, sequence: 1, price: 8,
      ))

      expect(tick.contract_id).to be == 1802875
      expect(tick.ms_of_day).to be == 53791000
      expect(tick.sequence).to be == 2
      expect(tick.price).to be == 550572
      expect(tick.price_decimal).to be == BigDecimal("5505.72")

      tick.apply_changes(changes(
        contract_id: 1802875, ms_of_day: 1000, sequence: 1, price: -7,
      ))

      expect(tick.price).to be == 550565
      expect(tick.price_decimal).to be == BigDecimal("5505.65")
    end
  end
end
