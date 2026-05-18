from __future__ import annotations

import math

from datetime import date
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import computed_field
from pydantic import field_validator

from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import WebSearchTool


class Transaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    refined_id: int | None = Field(
        default=None,
        description="Primary key of the row in transactions_rfn",
    )
    Date: date | None = Field(description="date of the transaction")
    merchant: str | None = Field(
        default=None,
        description="Normalized merchant name from refined ingestion",
    )
    booking_text: str = Field(
        alias="Booking text", description="bboking text of the transaction"
    )
    zkb_reference: str | None = Field(
        alias="ZKB reference", description="reference number of the transaction"
    )
    balance_chf: float | None = Field(
        alias="Balance CHF", description="balance of account after transaction in CHF"
    )
    amount_chf: float | None = Field(
        description="amount of the transaction in CHF, negative if expense, positive if income"
    )
    is_person: bool = Field(
        default=False,
        description="Whether the booking text indicates a person-to-person transaction",
    )

    @field_validator("Date", mode="before")
    @classmethod
    def parse_date(cls, value: str | None) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        else:
            return datetime.strptime(value, "%d.%m.%Y").date()

    @field_validator("zkb_reference", mode="before")
    @classmethod
    def parse_zkb_reference(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    @computed_field
    def transaction_type(self) -> str:
        if self.amount_chf is None:
            return "expense"
        if self.amount_chf > 0:
            return "income"
        elif self.amount_chf < 0:
            return "expense"
        return "expense"

    @computed_field
    def amount_chf_abs(self) -> float | None:
        if self.amount_chf is None:
            return None
        return abs(self.amount_chf)


class CategoryMain(StrEnum):
    SPORT = "Sport"  # Sport, Tennis, Golf, Padel, Migros Golfpark
    ENTERTAINMENT = "Entertainment"  # Spotify, Festival, concert, cinema, theatre
    TELECOMMUNICATION = "Telecommunication"  # GoMo, Sunrise, Salt
    RESTAURANT = "Restaurant"  # Restaurants, don't include Grociers
    HEALTHCARE = "Healthcare"  # Sanitas, medicine, Apotheke, Dentist
    GOVERNMENT = "Government"  # Taxes
    RETAIL = "Retail"  # Manor, Ochsner Sport, Ochnser Shoes. All kinds of stores to buy clothes. No Grocery stores.
    GROCERIES = "Groceries"  # Migros, Coop, Aldi, LIDL, migrolino, Denner
    SALARY = "Salary"  # Salary from the company
    HOUSING = "Housing"  # Rent
    CAR = "Car"  # Car purchase, service, gas station, parking
    TRANSPORT = "Transport"  # Train, Bus, SBB, Deutsche Bahn, all transportation except flights.
    TRAVEL = "Travel"  # accomodation, flight, car rental, booking.com, Airbnb, Hotel
    INSURANCE = "Insurance"  # car insurance, health insurance, other insurances
    EDUCATION = "Education"  # online classes and courses. Udemly
    PAYMENT_SERVICES = "Payment Services"  # credit card payments services
    INVESTING = "Investing"  # Revolut, TrueWealth
    POSTAL_SERVICES = "Postal Services"  # Swiss Post, parcel shipping, letters, stamps
    FRIEND = "Friend"  # All bookings with personal names and/or a phone number. (Remove phone number)


class CategorySecond(StrEnum):
    # SPORT
    # Use only for sports activities, sport clubs, sport equipment and related memberships.
    SPORT_TENNIS = "Tennis"
    SPORT_GOLF = "Golf"
    SPORT_PADEL = "Padel"
    SPORT_BIKE = "Bike"
    SPORT_FITNESS = "Fitness"
    SPORT_RUNNING = "Running"
    SPORT_SWIMMING = "Swimming"

    # ENTERTAINMENT
    # Use for leisure/consumption entertainment (streaming, events, cinema, theatre, spectator sports).
    ENTERTAINMENT_MUSIC = "Music Streaming"
    ENTERTAINMENT_EVENTS = "Events & Concerts"
    ENTERTAINMENT_SPORTS = "Sports"
    ENTERTAINMENT_CINEMA = "Cinema"
    ENTERTAINMENT_THEATRE = "Theatre"
    ENTERTAINMENT_TV = "TV & Streaming"
    ENTERTAINMENT_DATING = "Dating"
    ENTERTAINMENT_AMUSEMENT_PARK = "Amusement Park"

    # TELECOMMUNICATION
    # Use for telecom contracts and providers (mobile/internet/tv subscriptions).
    TELECOM_MOBILE = "Mobile"
    TELECOM_INTERNET = "Internet"
    TELECOM_TV = "TV Subscription"

    # RESTAURANT
    # Use for food & drink consumption (eat-in, takeaway, cafe, bar, delivery).
    # Takeaway should be categorized as RESTAURANT_FASTFOOD
    # items which contain, bar/pub should be categorized as RESTAURANT_BAR
    # items which contain cafe or bakery should be categorized as RESTAURANT_CAFE
    RESTAURANT_DINING = "Dining"
    RESTAURANT_FASTFOOD = "Fast Food"
    RESTAURANT_CAFE = "Cafe"
    RESTAURANT_BAR = "Bar"
    RESTAURANT_DELIVERY = "Food Delivery"

    # HEALTHCARE
    # Use for medical care, treatment, pharmacy and personal care providers.
    # Health-insurance products belong to INSURANCE_* (not healthcare providers).
    HEALTHCARE_DOCTOR = "Doctor"
    HEALTHCARE_DENTIST = "Dentist"
    HEALTHCARE_HAIR = "Hairdresser"
    HEALTHCARE_OPTICIAN = "Optician"
    HEALTHCARE_PHARMACY = "Pharmacy"
    HEALTHCARE_THERAPY = "Therapy"
    HEALTHCARE_INSURANCE = "Health Insurance"
    HEALTHCARE_SPA = "Spa & Wellness"

    # GOVERNMENT
    # Use for taxes, public fees, fines and administrative public services.
    GOVERNMENT_TAX = "Taxes"
    GOVERNMENT_FEES = "Government Fees"
    GOVERNMENT_FINES = "Fines"
    GOVERNMENT_VEHICLE_REGISTRATION = "Vehicle Registration"
    GOVERNMENT_ADMIN = "Administrative Services"

    # RETAIL
    # Use for non-grocery consumer goods stores (clothing, electronics, furniture, etc.).
    RETAIL_CLOTHING = "Clothing"
    RETAIL_SPORTS = "Sport"
    RETAIL_ELECTRONICS = "Electronics"
    RETAIL_HOME = "Home Goods"
    RETAIL_GARDEN = "Garden"
    RETAIL_DEPARTMENT = "Department Store"
    RETAIL_SHOES = "Shoes"
    RETAIL_FURNITURE = "Furniture"
    RETAIL_DRUG_STORE = "Drug Store"
    RETAIL_PHOTOGRAPHY = "Photography"
    RETAIL_BOOK_STORE = "Book Store"

    # GROCERIES
    # Use for food shopping and drink retailers.
    GROCERIES_SUPERMARKET = "Supermarket"
    GROCERIES_SPECIALTY = "Specialty Food"
    GROCERIES_BAKERY = "Bakery"
    GROCERIES_DRINKS = "Drinks"
    GROCERIES_VENDING = "Vending Machine"

    # SALARY
    # Use only for employer salary inflows.
    SALARY_MAIN = "Main Salary"
    SALARY_BONUS = "Bonus"
    SALARY_DONATION = "Donation"

    # HOUSING
    # Use for rent, utilities and housing maintenance.
    HOUSING_RENT = "Rent"
    HOUSING_DEPOSIT = "Deposit"
    HOUSING_UTILITIES = "Utilities"
    HOUSING_MAINTENANCE = "Maintenance"

    # CAR
    # Use for vehicle operation and ownership costs (fuel, parking, service, wash, car taxes).
    # Car INSURANCE premiums belong to INSURANCE_CAR, not CAR_*.
    CAR_PURCHASE = "Purchase"
    CAR_SERVICE = "Service & Repair"
    CAR_FUEL = "Fuel"
    CAR_PARKING = "Parking"
    CAR_WASH = "Wash"
    CAR_TAX = "Tax"

    # TRANSPORT
    # Use for public/paid transport rides and tickets (train, bus, taxi, metro, ferry, bike rental).
    # Long-distance travel bookings (flight/hotel/car rental for trips) belong to TRAVEL_*.
    # Parking fees belong to CAR_PARKING, not TRANSPORT_PARKING.
    # Fuel always belongs to CAR_FUEL, not TRANSPORT_FUEL, even if it's for a trip. We want to capture fuel expenses under car ownership, not travel.
    TRANSPORT_TRAIN = "Train"
    TRANSPORT_BUS = "Bus"
    TRANSPORT_TAXI = "Taxi"
    TRANSPORT_METRO = "Metro"
    TRANSPORT_BIKE = "Bike Rental"
    TRANSPORT_FERRY = "Ferry"
    TRANSPORT_CABLE_CAR = "Cable Car"

    # TRAVEL
    # Use for trip planning and travel bookings (flight, accommodation, car rental, ticket platforms).
    TRAVEL_FLIGHT = "Flight"
    TRAVEL_HOTEL = "Hotel"
    TRAVEL_HOSTEL = "Hostel"
    TRAVEL_APARTMENT = "Apartment"
    TRAVEL_CAR_RENTAL = "Car Rental"
    TRAVEL_TICKET = "Ticket Booking"

    # INSURANCE
    # Use for ALL insurance premiums/policies and insurance providers.
    # Example: car insurance -> INSURANCE_CAR (not CAR_*).
    INSURANCE_HEALTH = "Health Insurance"
    INSURANCE_CAR = "Car Insurance"
    INSURANCE_HOME = "Home Insurance"
    INSURANCE_TRAVEL = "Travel Insurance"

    # EDUCATION
    # Use for courses, learning platforms and educational materials.
    EDUCATION_ONLINE = "Online Courses"
    EDUCATION_BOOKS = "Books"

    # PAYMENT SERVICES
    # Use for payment network/service fees, FX fees and transfer fees.
    PAYMENT_FEES = "Payment Fees"
    PAYMENT_CURRENCY = "Currency Exchange"
    PAYMENT_TRANSFER = "Transfer Fees"
    PAYMENT_MONEY_TRANSFER = "Money Transfer"

    # INVESTING
    # Use for brokerage/investment platform related transactions.
    INVESTING_BROKERAGE = "Brokerage"

    # POSTAL SERVICES
    # Use for postal and shipping services (sending letters, parcels, buying stamps).
    POSTAL_POST_OFFICE = "Post Office"

    # FRIEND
    # Use for person-to-person transfers, reimbursements and bill splits.
    FRIEND_SUPPORT_PAYMENT = "Support Payment"


class MerchantMetaData(BaseModel):
    name: str = Field(description="The name of the Merchant")

    category_main: CategoryMain | None = Field(
        description="Choose the most appropriate main merchant category"
    )
    category_second: CategorySecond | str | None = Field(
        description="Choose the second category which belongs to one category_main. If none matches you can create your own one."
    )
    city: str | None = Field(description="City/town from the booking text")


class MerchantExtractor(BaseModel):
    merchant: str = Field(description="Name of the Merchant")
    is_person: bool = Field(description="Whether the merchant is a person or a company")


class MerchantMetaInput(BaseModel):
    merchant: str = Field(description="Name of the merchant")
    booking_text: str = Field(
        description="Booking text of the transaction which includes zip code and town/city"
    )
    merchant_summary: str = Field(description="Summary of the Merchant")


class MetadataResult(BaseModel):
    current_datetime: datetime
    zkb_reference: str | None
    matched_merchant: str
    cache_hit: bool
    similarity: float | None
    search_tool: WebSearchTool | None
    category_main: str | None
    category_second: str | None
    city: str | None


# known merchant brands for merchant normalization
MERCHANT_BRANDS = [
    "coop",
    "migros",
    "aldi",
    "lidl",
    "denner",
    "migrolino",
    "sbb",
    "manor",
    "ikea",
    "mediamarkt",
    "zalando",
]

# Compound brand patterns: checked BEFORE simple brand matching.
# Longer/more specific patterns must come first.
# Maps a substring pattern → canonical merchant key.
# Use this for brands that have distinct sub-brands with different categories.
MERCHANT_COMPOUND_BRANDS: list[tuple[str, str]] = [
    # Migros sub-brands — all different categories, not grocery stores
    ("migros golfpark", "migros golfpark"),  # Sport / Golf
    ("migros gp", "migros golfpark"),  # Sport / Golf (abbreviation)
    ("migros do it", "migros do it deco"),  # Retail / Home & Garden
    ("migros doit", "migros do it deco"),  # Retail / Home & Garden (no space)
    ("migros klubschule", "migros klubschule"),  # Education
    ("migros club school", "migros klubschule"),  # Education (english)
    ("migros fitnesscenter", "migros fitnesscenter"),  # Sport / Fitness
    ("migros fitness", "migros fitnesscenter"),  # Sport / Fitness (short)
    ("migros restaurant", "migros restaurant"),  # Restaurant
]
