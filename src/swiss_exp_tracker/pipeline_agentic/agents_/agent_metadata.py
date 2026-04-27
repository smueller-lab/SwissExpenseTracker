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

**Sport** (e.g. Tennis, Golf, Padel, Migros Golfpark)
Use only for sports activities, clubs, equipment and memberships.
Sub-categories: Tennis | Golf | Padel | Bike | Fitness | Running | Swimming

**Entertainment** (e.g. Spotify, Festival, concert, cinema, theatre)
Use for leisure/consumption entertainment (streaming, events, cinema, theatre, spectator sports).
Sub-categories: Music Streaming | Events & Concerts | Sports | Cinema | Theatre | TV & Streaming

**Telecommunication** (e.g. GoMo, Sunrise, Salt)
Use for telecom contracts and providers (mobile, internet, TV subscriptions).
Sub-categories: Mobile | Internet | TV Subscription

**Restaurant** (restaurants; not groceries)
Use for food & drink consumption (eat-in, takeaway, cafe, bar, delivery).
Takeaway → Fast Food | bar/pub → Bar | cafe/bakery → Cafe
Sub-categories: Dining | Fast Food | Cafe | Bar | Food Delivery

**Healthcare** (e.g. Sanitas, medicine, Apotheke, Dentist)
Use for medical care, treatment, pharmacy and personal care providers. Insurance products belong to Insurance, not Healthcare.
Sub-categories: Doctor | Dentist | Hairdresser | Optician | Pharmacy | Therapy | Health Insurance

**Government** (e.g. Taxes)
Use for taxes, public fees, fines and administrative public services.
Sub-categories: Taxes | Government Fees | Fines | Vehicle Registration | Administrative Services

**Retail** (e.g. Manor, Ochsner Sport, Ochsner Shoes — all kinds of stores for clothes/goods; no grocery stores)
Use for non-grocery consumer goods stores.
Sub-categories: Clothing | Sport | Electronics | Home Goods | Garden | Department Store | Shoes | Furniture | Drug Store | Photography

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
'Raststätte' is typical german word for highway service stations, which often include fuel, food and parking. Classify based on the dominant expense. If fuel is dominant → Car; if food is dominant → Restaurant; if parking is dominant → Car.
Sub-categories: Purchase | Service & Repair | Fuel | Parking | Wash | Tax

**Transport** (e.g. Train, Bus, SBB, Deutsche Bahn — all transportation except flights)
Use for public/paid transport rides and tickets. Long-distance trip bookings → Travel. Parking fees → Car. Fuel → Car.
Sub-categories: Train | Bus | Taxi | Metro | Bike Rental | Ferry | Cable Car 

**Travel** (e.g. accommodation, flight, car rental, booking.com, Airbnb, Hotel)
Use for trip planning and travel bookings (flight, accommodation, car rental, ticket platforms).
Sub-categories: Flight | Hotel | Hostel | Apartment | Car Rental | Ticket Booking

**Insurance** (e.g. car insurance, health insurance, other insurances)
Use for ALL insurance premiums and policies. Example: car insurance → Car Insurance sub-category.
Sub-categories: Health Insurance | Car Insurance | Home Insurance | Travel Insurance

**Education** (e.g. online classes and courses, Udemy)
Use for courses, learning platforms and educational materials.
Sub-categories: Online Courses | Books

**Payment Services** (e.g. credit card payment services)
Use for payment network/service fees, FX fees and transfer fees.
Sub-categories: Payment Fees | Currency Exchange | Transfer Fees

**Investing** (e.g. Revolut, TrueWealth)
Use for brokerage/investment platform related transactions.
Sub-categories: Brokerage

**Friend**
All bookings with personal names and/or a phone number (remove the phone number). Use for person-to-person transfers, reimbursements and bill splits.
Sub-categories: Support Payment

## Warnings
- You cannot classify groceries with Retail as category_main. Food is divided into Groceries and Restaurant.
- If a transaction is an insurance premium/policy, classify as Insurance (not Car/Healthcare). E.g. car insurance → Insurance.
- Use Car only for vehicle operation costs (fuel, parking, service, wash, car taxes).
- Use Travel for flight/hotel/car rental bookings, and Transport for local/public rides (train, bus, taxi, metro, ferry).
- Use Healthcare for care providers (doctor, dentist, pharmacy, therapy), not insurance products.
- Name of the Merchant and category_main cannot be None.
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
