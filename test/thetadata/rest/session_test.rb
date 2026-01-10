require "thetadata"

describe ThetaData::REST::Session do
  let(:session_id) { "5ffeaf47-9712-4b8f-9e1a-123456789abc" }
  let(:user) do
    {
      stockSubscription: "FULL",
      optionsSubscription: "FULL",
      indicesSubscription: "PRO",
    }
  end
  let(:session) { ThetaData::REST::Session.new(session_id: session_id, user: user) }

  it "stores session_id" do
    expect(session.session_id).to be == session_id
  end

  it "stores user" do
    expect(session.user).to be == user
  end

  it "records created_at" do
    expect(session.created_at).to be_a(Time)
    expect(Time.now - session.created_at).to be < 1
  end

  with "#valid?" do
    it "returns true for fresh session" do
      expect(session.valid?).to be == true
    end

    it "returns false for expired session" do
      old_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: user,
        created_at: Time.now - 4000,  # Over 1 hour ago
      )
      expect(old_session.valid?).to be == false
    end
  end

  with "#expired?" do
    it "returns false for fresh session" do
      expect(session.expired?).to be == false
    end

    it "returns true for old session" do
      old_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: user,
        created_at: Time.now - 4000,
      )
      expect(old_session.expired?).to be == true
    end
  end

  with "#subscription_tier" do
    it "returns the highest subscription tier" do
      expect(session.subscription_tier).to be == "PRO"
    end

    it "normalizes FULL to PRO" do
      full_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: "FULL" },
      )
      expect(full_session.subscription_tier).to be == "PRO"
    end

    it "normalizes PROFESSIONAL to PRO" do
      pro_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { indicesSubscription: "PROFESSIONAL" },
      )
      expect(pro_session.subscription_tier).to be == "PRO"
    end

    it "handles FREE tier" do
      free_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: "FREE", optionsSubscription: "FREE", indicesSubscription: "FREE" },
      )
      expect(free_session.subscription_tier).to be == "FREE"
    end

    it "handles integer tier values" do
      int_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: 2, optionsSubscription: 1, indicesSubscription: 3 },
      )
      expect(int_session.subscription_tier).to be == "PRO"
    end

    it "maps integer 0 to FREE" do
      int_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: 0 },
      )
      expect(int_session.subscription_tier).to be == "FREE"
    end

    it "maps integer 1 to VALUE" do
      int_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: 1 },
      )
      expect(int_session.subscription_tier).to be == "VALUE"
    end

    it "maps integer 2 to STANDARD" do
      int_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: { stockSubscription: 2 },
      )
      expect(int_session.subscription_tier).to be == "STANDARD"
    end

    it "returns FREE for nil user" do
      nil_session = ThetaData::REST::Session.new(
        session_id: session_id,
        user: nil,
      )
      expect(nil_session.subscription_tier).to be == "FREE"
    end
  end
end
