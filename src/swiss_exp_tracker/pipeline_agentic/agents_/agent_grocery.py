from __future__ import annotations

from typing import Any

from agents import Agent
from agents import GuardrailFunctionOutput
from agents import RunContextWrapper
from agents import output_guardrail

from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryData
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain


INSTRUCTIONS = """
## Role
You are an expert in categorising Swiss grocery products.

## Task
Given a grocery article name and the store it was purchased from, assign:
1. A `category_main` — the broad food/product group
2. A `category_detail` — the specific sub-category within that group

## Input
You receive a JSON object with two fields:
- `article_normalized`: cleaned product name (size/unit suffixes already removed)
- `location`: store branch where the item was purchased (context only)

## Output
Return a JSON object matching the GroceryCategoryData schema:
- `article`: repeat the `article_normalized` value unchanged
- `category_main`: one of the allowed main categories
- `category_detail`: one of the allowed detail categories that belongs to the chosen main

## Category Reference

**Fresh Produce** — raw fruit, vegetables, salad, herbs
Sub-categories: Fruits | Vegetables | Salad & Herbs

**Dairy & Eggs** — milk, cream, yogurt, cheese, eggs, butter, cream cheese
Sub-categories: Milk & Cream | Yogurt | Cheese | Eggs | Butter & Cream Cheese

**Bakery & Bread** — bread, rolls, croissants, pastries, cakes, tortilla wraps, flatbreads
Sub-categories: Bread & Rolls | Croissants & Pastries | Cakes & Desserts | Tortilla & Wraps

**Meat & Fish** — all animal proteins including plant-based alternatives
Sub-categories: Beef & Pork | Poultry | Fish & Seafood | Deli & Sausages | Vegan Meat

**Pasta & Grains** — pasta, noodles, rice, grains, breakfast cereals, muesli
Sub-categories: Pasta & Noodles | Rice & Grains | Cereals & Muesli

**Canned & Preserved** — canned goods, sauces, pesto, pasta sauce, oil, vinegar, pickles, jams, olives, hummus, dips
Sub-categories: Canned Vegetables & Legumes | Sauces & Condiments | Oils & Vinegar | Pickles, Jams & Spreads | Dips & Spreads

**Frozen Foods** — frozen meals, ice cream, frozen vegetables and fruits
Sub-categories: Frozen Meals | Ice Cream | Frozen Vegetables & Fruits

**Snacks & Sweets** — chips, chocolate, candy, cookies, nuts, dried fruits
Sub-categories: Chips & Pretzels | Chocolate & Candy | Cookies & Biscuits | Nuts & Dried Fruits

**Beverages** — water, soft drinks, juice, coffee, tea, beer
Sub-categories: Water | Soft Drinks | Juice | Coffee & Tea | Beer

**Ready Meals** — prepared meals, pizza, flatbread, soups, stews
Sub-categories: Prepared Meals | Pizza & Flatbread | Soups & Stews

**Personal & Household** — cleaning products, personal care, baby products
Sub-categories: Cleaning Products | Personal Care | Baby Products

**Baking** — raw baking ingredients (flour, sugar, yeast, baking powder, starch, vanilla, cocoa powder)
Sub-categories: Flour & Starch | Sugar & Sweeteners | Baking Aids

**Other** — anything that does not fit the categories above
Sub-categories: Unknown

## Rules
- `category_main` must be exactly one of the listed main categories.
- `category_detail` must belong to the chosen `category_main`.
- If truly uncertain, use Other / Unknown.
- Swiss/German product names: translate mentally to identify the product type.
  Examples: "Vollmilch" = whole milk → Dairy & Eggs / Milk & Cream
            "Karotten" = carrots → Fresh Produce / Vegetables
            "Erdbeeren" = strawberries → Fresh Produce / Fruits
            "Croissant" → Bakery & Bread / Croissants & Pastries
"""


@output_guardrail
async def _guardrail_category_main(
    ctx: RunContextWrapper[None], agent: Agent, output: GroceryCategoryData
) -> GuardrailFunctionOutput:
    valid = sorted(c.value for c in GroceryCategoryMain)
    category = getattr(output, "category_main", None)

    info: dict[str, Any] = {"allowed_categories": valid}
    errors: list[str] = []

    if category is None:
        errors.append("category_main is missing")
    elif isinstance(category, list):
        errors.append("category_main must be a single value, not a list")
    elif isinstance(category, str) and category not in valid:
        errors.append(f"Invalid category_main: '{category}'")
    elif not isinstance(category, str | GroceryCategoryMain):
        errors.append(f"Unexpected type for category_main: {type(category).__name__}")

    if errors:
        info["errors"] = errors
        info["received_value"] = category
        return GuardrailFunctionOutput(output_info=info, tripwire_triggered=True)

    return GuardrailFunctionOutput(output_info=info, tripwire_triggered=False)


grocery_agent = Agent(
    name="Grocery Category Agent",
    instructions=INSTRUCTIONS,
    model="gpt-4o-mini",
    output_type=GroceryCategoryData,
    output_guardrails=[_guardrail_category_main],
)
