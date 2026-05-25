from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel
from pydantic import Field


class GroceryCategoryMain(StrEnum):
    FRESH_PRODUCE = "Fresh Produce"
    DAIRY_EGGS = "Dairy & Eggs"
    BAKERY_BREAD = "Bakery & Bread"
    MEAT_FISH = "Meat & Fish"
    PASTA_GRAINS = "Pasta & Grains"
    CANNED_PRESERVED = "Canned & Preserved"
    FROZEN_FOODS = "Frozen Foods"
    SNACKS_SWEETS = "Snacks & Sweets"
    BEVERAGES = "Beverages"
    READY_MEALS = "Ready Meals"
    PERSONAL_HOUSEHOLD = "Personal & Household"
    BAKING = "Baking"
    OTHER = "Other"


class GroceryCategoryDetail(StrEnum):
    # Fresh Produce
    FRUITS = "Fruits"
    VEGETABLES = "Vegetables"
    SALAD_HERBS = "Salad & Herbs"
    # Dairy & Eggs
    MILK_CREAM = "Milk & Cream"
    YOGURT = "Yogurt"
    CHEESE = "Cheese"
    EGGS = "Eggs"
    BUTTER_CREAM_CHEESE = "Butter & Cream Cheese"
    # Bakery & Bread
    BREAD_ROLLS = "Bread & Rolls"
    CROISSANTS_PASTRIES = "Croissants & Pastries"
    CAKES_DESSERTS = "Cakes & Desserts"
    TORTILLA_WRAPS = "Tortilla & Wraps"
    # Meat & Fish
    BEEF_PORK = "Beef & Pork"
    POULTRY = "Poultry"
    FISH_SEAFOOD = "Fish & Seafood"
    DELI_SAUSAGES = "Deli & Sausages"
    VEGAN_MEAT = "Vegan Meat"
    # Pasta & Grains
    PASTA_NOODLES = "Pasta & Noodles"
    RICE_GRAINS = "Rice & Grains"
    CEREALS_MUESLI = "Cereals & Muesli"
    # Canned & Preserved
    CANNED_VEG_LEGUMES = "Canned Vegetables & Legumes"
    SAUCES_CONDIMENTS = "Sauces & Condiments"
    OILS_VINEGAR = "Oils & Vinegar"
    PICKLES_JAMS = "Pickles, Jams & Spreads"
    DIPS_SPREADS = "Dips & Spreads"
    # Frozen Foods
    FROZEN_MEALS = "Frozen Meals"
    ICE_CREAM = "Ice Cream"
    FROZEN_VEG_FRUITS = "Frozen Vegetables & Fruits"
    # Snacks & Sweets
    CHIPS_PRETZELS = "Chips & Pretzels"
    CHOCOLATE_CANDY = "Chocolate & Candy"
    COOKIES_BISCUITS = "Cookies & Biscuits"
    NUTS_DRIED_FRUITS = "Nuts & Dried Fruits"
    # Beverages
    WATER = "Water"
    SOFT_DRINKS = "Soft Drinks"
    JUICE = "Juice"
    COFFEE_TEA = "Coffee & Tea"
    BEER = "Beer"
    # Ready Meals
    PREPARED_MEALS = "Prepared Meals"
    PIZZA_FLATBREAD = "Pizza & Flatbread"
    SOUPS_STEWS = "Soups & Stews"
    # Personal & Household
    CLEANING = "Cleaning Products"
    PERSONAL_CARE = "Personal Care"
    BABY = "Baby Products"
    # Baking
    FLOUR_STARCH = "Flour & Starch"
    SUGAR_SWEETENERS = "Sugar & Sweeteners"
    BAKING_AIDS = "Baking Aids"
    # Other
    UNKNOWN = "Unknown"


class GroceryCategoryData(BaseModel):
    """LLM output: category assigned to one grocery article."""

    article: str = Field(description="The normalised article name")
    category_main: GroceryCategoryMain = Field(description="Main grocery category")
    category_detail: GroceryCategoryDetail = Field(
        description="Detailed sub-category within category_main"
    )


class GroceryRow(BaseModel):
    """Input to the agentic pipeline, loaded from groceries_rfn."""

    rfn_id: int = Field(description="Primary key in groceries_rfn")
    article: str = Field(description="Original article name from receipt")
    article_normalized: str = Field(
        description="Cleaned article name for vector lookup"
    )
    location: str = Field(description="Store branch (Filiale)")


class GroceryCategoryResult(BaseModel):
    """Written to grocery_categorization_raw in SQLite after each categorisation."""

    current_datetime: datetime
    rfn_id: int
    article: str
    matched_article: str
    cache_hit: bool
    similarity: float | None
    category_main: GroceryCategoryMain
    category_detail: GroceryCategoryDetail
