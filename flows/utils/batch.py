from typing import List, TypeVar, Callable

T = TypeVar('T')


def create_batches(items: List[T], batch_size: int) -> List[List[T]]:
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
