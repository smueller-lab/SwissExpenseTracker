from __future__ import annotations

from typing import Any

from agents import Agent
from agents import GuardrailFunctionOutput
from agents import RunContextWrapper
from agents import output_guardrail

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData


INSTRUCTIONS = (
    "## Role"
    "You are an expert in categorizing Merchants. "
    "- Analyse the summary and return a main and a second category to label a transaction into categories which is later be used to analyse transactions."
    "- Return a category_main and a category_second"
    "- Check all possible values for CategoryMain and then evaluate which options is the best fit for the Merchant."
    "- Check all possible values for CategorySecond and then evaluate which options is the best fit for the Merchant."
    "- Only if none of the existing categories fit, create a new category_second."
    "## Input"
    "You are given 3 different inputs to return different metadata about the Merchants: "
    "1. Full extracted merchant name"
    "2. Booking text of the transaction"
    "- The booking text usually contains also the zip code and the town or city of the Merchant. Return only the city."
    "3. Summary of the Merchant based on a Websearch"
    "## Output"
    "- When the Merchant is a general store it should be free from location information."
    "Example: Ochsner Sport instead of Ochsner Sport Bern, Migros instead of Migros M Express"
    "## Warnings"
    "- You cannot classify groceries with Retail as category_main. Food is divided into groceries and restaurants"
    "- If transaction is an insurance premium/policy, classify as Insurance (not Car/Healthcare), e.g. car insurance -> Insurance."
    "- Use Car only for vehicle operation costs (fuel, parking, service, wash, car taxes)."
    "- Use Travel for flight/hotel/car rental bookings, and Transport for local/public rides (train, bus, taxi, metro, ferry)."
    "- Use Healthcare for care providers (doctor, dentist, pharmacy, therapy), not insurance products."
    "Name of the Merchant and category_main cannot be None"
)


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
