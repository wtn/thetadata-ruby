require "thetadata"

describe ThetaData::Streaming::DisconnectReason do
  let(:dr) { ThetaData::Streaming::DisconnectReason }

  with "all constants" do
    it "defines UNSPECIFIED as -1" do
      expect(dr::UNSPECIFIED).to be == -1
    end

    it "defines INVALID_CREDENTIALS as 0" do
      expect(dr::INVALID_CREDENTIALS).to be == 0
    end

    it "defines INVALID_LOGIN_VALUES as 1" do
      expect(dr::INVALID_LOGIN_VALUES).to be == 1
    end

    it "defines INVALID_LOGIN_SIZE as 2" do
      expect(dr::INVALID_LOGIN_SIZE).to be == 2
    end

    it "defines GENERAL_VALIDATION_ERROR as 3" do
      expect(dr::GENERAL_VALIDATION_ERROR).to be == 3
    end

    it "defines TIMED_OUT as 4" do
      expect(dr::TIMED_OUT).to be == 4
    end

    it "defines CLIENT_FORCED_DISCONNECT as 5" do
      expect(dr::CLIENT_FORCED_DISCONNECT).to be == 5
    end

    it "defines ACCOUNT_ALREADY_CONNECTED as 6" do
      expect(dr::ACCOUNT_ALREADY_CONNECTED).to be == 6
    end

    it "defines SESSION_TOKEN_EXPIRED as 7" do
      expect(dr::SESSION_TOKEN_EXPIRED).to be == 7
    end

    it "defines INVALID_SESSION_TOKEN as 8" do
      expect(dr::INVALID_SESSION_TOKEN).to be == 8
    end

    it "defines FREE_ACCOUNT as 9" do
      expect(dr::FREE_ACCOUNT).to be == 9
    end

    it "defines TOO_MANY_REQUESTS as 12" do
      expect(dr::TOO_MANY_REQUESTS).to be == 12
    end

    it "defines NO_START_DATE as 13" do
      expect(dr::NO_START_DATE).to be == 13
    end

    it "defines LOGIN_TIMED_OUT as 14" do
      expect(dr::LOGIN_TIMED_OUT).to be == 14
    end

    it "defines SERVER_RESTARTING as 15" do
      expect(dr::SERVER_RESTARTING).to be == 15
    end

    it "defines SESSION_TOKEN_NOT_FOUND as 16" do
      expect(dr::SESSION_TOKEN_NOT_FOUND).to be == 16
    end

    it "defines SERVER_USER_DOES_NOT_EXIST as 17" do
      expect(dr::SERVER_USER_DOES_NOT_EXIST).to be == 17
    end

    it "defines INVALID_CREDENTIALS_NULL_USER as 18" do
      expect(dr::INVALID_CREDENTIALS_NULL_USER).to be == 18
    end
  end

  with "NAMES" do
    it "contains all 18 constants" do
      expect(dr::NAMES.size).to be == 18
    end

    it "maps every constant to its name string" do
      dr::NAMES.each do |code, name|
        expect(dr.const_get(name)).to be == code
      end
    end
  end

  with ".name" do
    it "returns correct name for every known code" do
      {
        -1 => "UNSPECIFIED",
        0 => "INVALID_CREDENTIALS",
        1 => "INVALID_LOGIN_VALUES",
        2 => "INVALID_LOGIN_SIZE",
        3 => "GENERAL_VALIDATION_ERROR",
        4 => "TIMED_OUT",
        5 => "CLIENT_FORCED_DISCONNECT",
        6 => "ACCOUNT_ALREADY_CONNECTED",
        7 => "SESSION_TOKEN_EXPIRED",
        8 => "INVALID_SESSION_TOKEN",
        9 => "FREE_ACCOUNT",
        12 => "TOO_MANY_REQUESTS",
        13 => "NO_START_DATE",
        14 => "LOGIN_TIMED_OUT",
        15 => "SERVER_RESTARTING",
        16 => "SESSION_TOKEN_NOT_FOUND",
        17 => "SERVER_USER_DOES_NOT_EXIST",
        18 => "INVALID_CREDENTIALS_NULL_USER",
      }.each do |code, expected_name|
        expect(dr.name(code)).to be == expected_name
      end
    end

    it "returns UNKNOWN for unknown code" do
      expect(dr.name(999)).to be == "UNKNOWN(999)"
    end

    it "returns UNKNOWN for gap codes 10 and 11" do
      expect(dr.name(10)).to be == "UNKNOWN(10)"
      expect(dr.name(11)).to be == "UNKNOWN(11)"
    end
  end

  with ".reconnectable?" do
    it "returns true for exactly TIMED_OUT, SESSION_TOKEN_EXPIRED, SERVER_RESTARTING" do
      expect(dr.reconnectable?(dr::TIMED_OUT)).to be == true
      expect(dr.reconnectable?(dr::SESSION_TOKEN_EXPIRED)).to be == true
      expect(dr.reconnectable?(dr::SERVER_RESTARTING)).to be == true
    end

    it "has exactly 3 reconnectable codes" do
      expect(dr::RECONNECTABLE.size).to be == 3
    end

    it "returns false for all non-reconnectable codes" do
      non_reconnectable = [
        dr::UNSPECIFIED,
        dr::INVALID_CREDENTIALS,
        dr::INVALID_LOGIN_VALUES,
        dr::INVALID_LOGIN_SIZE,
        dr::GENERAL_VALIDATION_ERROR,
        dr::CLIENT_FORCED_DISCONNECT,
        dr::ACCOUNT_ALREADY_CONNECTED,
        dr::INVALID_SESSION_TOKEN,
        dr::FREE_ACCOUNT,
        dr::TOO_MANY_REQUESTS,
        dr::NO_START_DATE,
        dr::LOGIN_TIMED_OUT,
        dr::SESSION_TOKEN_NOT_FOUND,
        dr::SERVER_USER_DOES_NOT_EXIST,
        dr::INVALID_CREDENTIALS_NULL_USER,
      ]

      non_reconnectable.each do |code|
        expect(dr.reconnectable?(code)).to be == false
      end
    end

    it "returns false for unknown codes" do
      expect(dr.reconnectable?(999)).to be == false
    end
  end
end
