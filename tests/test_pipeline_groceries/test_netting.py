from __future__ import annotations

import pytest

from swiss_exp_tracker.pipeline_ingestion.stages.groceries.stage_03_rfn import (
    _ReceiptItem,
)
from swiss_exp_tracker.pipeline_ingestion.stages.groceries.stage_03_rfn import (
    net_receipt_rows,
)


def _item(
    raw_id: int,
    article: str,
    quantity: float,
    price: float = 1.0,
    discount: float = 0.0,
) -> _ReceiptItem:
    return _ReceiptItem(
        raw_id=raw_id,
        article=article,
        article_normalized=article.lower(),
        quantity=quantity,
        price=price,
        discount=discount,
        unit="qty",
    )


def test_single_item_passthrough() -> None:
    items = [_item(1, "Erdbeeren", 1.0, price=3.00)]
    result = net_receipt_rows(items)
    assert len(result) == 1
    assert result[0].quantity == pytest.approx(1.0)
    assert result[0].price == pytest.approx(3.00)
    assert result[0].raw_ids == [1]


def test_partial_return_keeps_net_positive() -> None:
    items = [
        _item(1, "Haribo", 2.0, price=2.50),
        _item(2, "Haribo", -1.0, price=-1.25),
    ]
    result = net_receipt_rows(items)
    assert len(result) == 1
    assert result[0].quantity == pytest.approx(1.0)
    assert result[0].price == pytest.approx(1.25)
    assert set(result[0].raw_ids) == {1, 2}


def test_full_return_discards_item() -> None:
    items = [
        _item(1, "Pasta", 1.0, price=1.50),
        _item(2, "Pasta", -1.0, price=-1.50),
    ]
    result = net_receipt_rows(items)
    assert result == []


def test_negative_net_discarded_with_warning(capfd: pytest.CaptureFixture[str]) -> None:
    items = [
        _item(1, "Cola", -2.0, price=-3.00),
        _item(2, "Cola", 1.0, price=1.50),
    ]
    result = net_receipt_rows(items)
    assert result == []
    captured = capfd.readouterr()
    assert "WARN" in captured.out or "WARN" in captured.err


def test_multiple_distinct_articles() -> None:
    items = [
        _item(1, "Milch", 1.0, price=1.80),
        _item(2, "Brot", 2.0, price=3.20),
        _item(3, "Milch", -1.0, price=-1.80),
    ]
    result = net_receipt_rows(items)
    articles = {r.article for r in result}
    assert articles == {"Brot"}
    brot = next(r for r in result if r.article == "Brot")
    assert brot.quantity == pytest.approx(2.0)


def test_price_summed_correctly() -> None:
    items = [
        _item(1, "Joghurt", 3.0, price=6.00, discount=-0.60),
        _item(2, "Joghurt", -1.0, price=-2.00, discount=0.20),
    ]
    result = net_receipt_rows(items)
    assert len(result) == 1
    assert result[0].quantity == pytest.approx(2.0)
    assert result[0].price == pytest.approx(4.00)
    assert result[0].discount == pytest.approx(-0.40)


def test_empty_input_returns_empty() -> None:
    assert net_receipt_rows([]) == []


def test_single_raw_id_stored_directly() -> None:
    items = [_item(42, "Apfel", 1.0)]
    result = net_receipt_rows(items)
    assert result[0].raw_ids == [42]


def test_multi_raw_id_collected() -> None:
    items = [_item(10, "Wasser", 2.0), _item(11, "Wasser", 1.0)]
    result = net_receipt_rows(items)
    assert sorted(result[0].raw_ids) == [10, 11]
