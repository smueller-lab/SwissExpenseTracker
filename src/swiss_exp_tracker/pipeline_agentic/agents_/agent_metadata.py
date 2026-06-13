from __future__ import annotations

from typing import Any

from agents import Agent
from agents import GuardrailFunctionOutput
from agents import RunContextWrapper
from agents import output_guardrail

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData

INSTRUCTIONS = """
## Role
You are an expert in categorizing Merchants.
- Analyse the summary and return a main and a second category to label a transaction into categories which is later used to analyse transactions.
- Return a category_main and a category_second.
- Check all possible values for CategoryMain and then evaluate which option is the best fit for the Merchant.
- Check all possible values for CategorySecond and then evaluate which option is the best fit for the Merchant.
- Only if none of the existing categories fit, create a new category_second.

## Input
You are given 3 different inputs to return different metadata about the Merchant:
1. Full extracted merchant name
2. Booking text of the transaction
   - The booking text usually contains the zip code and the town or city of the Merchant. Return only the city.
3. Summary of the Merchant based on a web search

## Output
- When the Merchant is a general store it should be free from location information.
  Example: Ochsner Sport instead of Ochsner Sport Bern, Migros instead of Migros M Express

## Categories
Use the following reference to select category_main and category_second.
Each entry lists the main category, typical examples, usage guidance, and valid sub-categories.

**Sport** (e.g. Tennis, Golf, Padel, Golfpark)
Use only for sports activities, clubs, and memberships that was executed by the person the transaction belongs to.
A sporting event was not executed by the person, so it belongs to Entertainment, not Sport. E.g. a ticket for a football match or a concert in a stadium belongs to Entertainment, not Sport.
Any sporting goods store or merchandising belongs to Retail not Sport. E.g. Ochsner Sport → Retail, not Sport; but a golf club membership or tennis club membership → Sport.
Sub-categories: Tennis | Golf | Padel | Bike | Fitness | Running | Swimming

**Entertainment** (e.g. Spotify, Festival, concert, cinema, theatre, sports event)
Use for leisure/consumption entertainment (streaming, events, cinema, theatre, spectator sports).
Any visit in a sports stadium or hall for watching an event belongs to Entertainment, not Sport.
Any visit to a Bar, Restaurant or Food Court inside a stadium or sports hall belongs to Restaurant, not Entertainment.
Places for pool, billiard, arcade games, bowling, darts, escape room go under Entertainment, Game Hall.
Sub-categories: Music Streaming | Events & Concerts | Sports | Cinema | Theatre | TV & Streaming | Dating | Amusement Park | Game Hall

**Telecommunication** (e.g. GoMo, Sunrise, Salt)
Use ONLY for internet providers and phone/mobile providers (mobile, internet, TV subscriptions from a telecom operator).
Do NOT use for banks, financial institutions, or any merchant whose name contains words like Bank, Banque, Banca, Sparkasse, Kantonalbank, or Raiffeisen — those always belong to Payment Services.
Sub-categories: Mobile | Internet | TV Subscription

**Restaurant** (restaurants; not groceries)
Any place that servers food and drinks go to Restaurant, and any place with food should be Dining like food hall, food court.
Use for food & drink consumption always go to Restaurant (eat-in, takeaway, Cafe, Bar, delivery).
Every Bar belongs to Restaurants also when it's not a Restaurant itself but the main category is still Restaurant.
Takeaway → Fast Food | bar/pub → Bar | cafe/bakery → Cafe
Sub-categories: Dining | Fast Food | Cafe | Bar | Food Delivery

**Healthcare** (e.g. Sanitas, medicine, Apotheke, Dentist)
Use for medical care, treatment, pharmacy and personal care providers. Insurance products belong to Insurance, not Healthcare.
Spa & Welness like Spa, Sauna, Massage belongs to Healthcare.
Sub-categories: Doctor | Dentist | Hairdresser | Optician | Pharmacy | Therapy | Health Insurance | Spa & Wellness

**Government** (e.g. Taxes)
Use for taxes, public fees, fines and administrative public services.
Sub-categories: Taxes | Government Fees | Fines | Vehicle Registration | Administrative Services

**Retail** (e.g. Manor, Ochsner Sport, Ochsner Shoes — all kinds of stores for clothes/goods; no grocery stores)
Use for non-grocery consumer goods stores.
Sub-categories: Clothing | Sport | Electronics | Home Goods | Garden | Department Store | Shoes | Furniture | Drug Store | Photography | Book store

**Groceries** (e.g. Migros, Coop, Aldi, LIDL, migrolino, Denner)
Use for food shopping and drink retailers. Never classify groceries as Retail.
Sub-categories: Supermarket | Specialty Food | Bakery | Drinks | Vending Machine

**Salary** (e.g. salary from the company)
Use only for employer salary inflows.
Sub-categories: Main Salary | Bonus

**Housing** (e.g. Rent)
Use for rent, utilities and housing maintenance.
Sub-categories: Rent | Utilities | Maintenance

**Car** (e.g. car purchase, service, gas station, parking)
Use for vehicle operation and ownership costs (fuel, parking, service, wash, car taxes). Car insurance premiums belong to Insurance, not Car.
'Raststätte', 'raststaette' is a typical german word for highway gas station, which often include fuel, food and parking. Classify based on the dominant expense. If fuel is dominant → Car; if food is dominant → Restaurant.
Transactions coming from a car dealership should be all classified  as 'Service & Repair'.
Category 'Purchase' can only be set manually as this is not extractable by the merchant name.
Sub-categories: Purchase | Service & Repair | Fuel | Parking | Wash | Tax

**Transport** (e.g. Train, Bus, SBB, Deutsche Bahn — all transportation except flights)
Use for public/paid transport rides and tickets. Long-distance trip bookings → Travel. Parking fees → Car. Fuel → Car.
Parking belongs to Car, never Transport, even if it's at a train station or airport.
Sub-categories: Train | Bus | Taxi | Metro | Bike Rental | Ferry | Cable Car 

**Travel** (e.g. accommodation, flight, car rental, booking.com, Airbnb, Hotel)
Use for trip planning and travel bookings (flight, accommodation, car rental, ticket platforms).
Travel packages of Flight + Hotel are classified as All Inclusive
Sub-categories: Flight | Hotel | Hostel | Apartment | Car Rental | Ticket Booking | All Inclusive

**Insurance** (e.g. car insurance, health insurance, other insurances)
Use for ALL insurance premiums and policies. Example: car insurance → Car Insurance sub-category.
Sub-categories: Health Insurance | Car Insurance | Home Insurance | Travel Insurance

**Education** (e.g. online classes and courses, Udemy)
Use for courses, learning platforms and educational materials.
Sub-categories: Online Courses | Books

**Payment Services** (e.g. credit card payment services)
Any transaction to belongs to a bank goes here under Payment Services.
Use for payment network/service fees, FX fees and transfer fees.
Sub-categories: Payment Fees | Currency Exchange | Transfer Fees | Money Transfer | Cash Withdrawl

**Investing** (e.g. Revolut, TrueWealth)
Use for brokerage/investment platform related transactions.
Sub-categories: Brokerage

**Postal Services** (e.g. Swiss Post, Die Post, La Poste)
Use for postal and shipping services (sending letters, parcels, buying stamps). Do not use for PostFinance financial transactions — those belong to Payment Services.
Sub-categories: Post Office

**Friend**
All bookings with personal names and/or a phone number (remove the phone number). Use for person-to-person transfers, reimbursements and bill splits.
Booking must contain a real persons name and cannot contain company indicators like GmbH, AG, Inc, etc. If the name contains an item like bakery, vital, jus or similar then it cannot be a person.
Examples: "Anna's Bakery" -> Bakery and not Friend, "fureinander" -> no real name but could be company, "peter max" -> real name so friend
Sub-categories: Support Payment

## Warnings
- You cannot classify groceries with Retail as category_main. Food is divided into Groceries and Restaurant.
- If a transaction is an insurance premium/policy, classify as Insurance (not Car/Healthcare). E.g. car insurance → Insurance.
- Use Car only for vehicle operation costs (fuel, parking, service, wash, car taxes).
- Use Travel for flight/hotel/car rental bookings, and Transport for local/public rides (train, bus, taxi, metro, ferry).
- Use Healthcare for care providers (doctor, dentist, pharmacy, therapy), not insurance products.
- Name of the Merchant and category_main cannot be None.
- When most of the components in summary are unknown, guess based on the merchant name"
- Sporting Goods & Merchandising always belong to RETAIL_SPORTS and not into the SPORT category. Only actual sports activities, clubs, equipment rentals and memberships belong to SPORT.
- If the merchant contains a personal name and the merchant summary does not indicate a specific store or is unknown, classify as Friend. Don't try to assume the it's a store"
- Any merchant whose name contains "Bank", "Banque", "Banca", "Sparkasse", "Kantonalbank", or "Raiffeisen" must be classified as Payment Services, never as Telecommunication or any other category.
"""


@output_guardrail
async def guardrail_category_main_validator(
    ctx: RunContextWrapper[None], agent: Agent, output: MerchantMetaData
) -> GuardrailFunctionOutput:
    """
    Output guardrail to ensure category_main is exactly one valid CategoryMain enum value.
    """

    category_main = getattr(output, "category_main", None)

    valid_values = sorted(c.value for c in CategoryMain)

    output_info: dict[str, Any] = {
        "allowed_categories": valid_values,
        "instruction": "Choose ONE value from allowed_categories and return it as category_main",
    }

    errors: list[str] = []

    # Case 1: Missing
    if category_main is None:
        errors.append("category_main is missing")

    # Case 2: List or multiple values
    elif isinstance(category_main, list):
        errors.append("category_main must be a single value, not a list")

    # Case 3: String but not a valid enum value
    elif isinstance(category_main, str) and category_main not in valid_values:
        errors.append(f"Invalid category_main: '{category_main}'")

    # Case 4: Wrong type entirely
    elif not isinstance(category_main, (str | CategoryMain)):
        errors.append(
            f"category_main must be one of the allowed enum values, got {type(category_main).__name__}"
        )

    # Attach errors if any
    if errors:
        output_info["errors"] = errors
        output_info["received_value"] = category_main

        return GuardrailFunctionOutput(
            output_info=output_info,
            tripwire_triggered=True,
        )

    # Passed all checks
    return GuardrailFunctionOutput(
        output_info={
            "category_main": category_main,
            "allowed_categories": valid_values,
        },
        tripwire_triggered=False,
    )


metadata_agent = Agent(
    name="Metadata Agent",
    instructions=INSTRUCTIONS,
    model="gpt-4o-mini",
    output_type=MerchantMetaData,
    output_guardrails=[guardrail_category_main_validator],
)
