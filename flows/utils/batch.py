from typing import List, TypeVar

T = TypeVar('T')


def split_evenly(lst: List[T], n: int) -> List[List[T]]:
    """Split lst into n roughly-equal chunks. If len(lst) < n, returns fewer chunks."""
    n = min(n, len(lst))
    if n == 0:
        return []
    k, m = divmod(len(lst), n)
    chunks, start = [], 0
    for i in range(n):
        end = start + k + (1 if i < m else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks


def create_batches(items: List[T], batch_size: int) -> List[List[T]]:
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


def create_batches_by_item_count(items: List[List[T]], max_items: int) -> List[List[List[T]]]:
    """Batch a list of lists so each batch's total inner-item count stays under max_items.

    Used for uidCarTerms where each element is one student's list of term records
    and max_items is the max total term records per batch.
    """
    batches: List[List[List[T]]] = []
    current_batch: List[List[T]] = []
    current_count = 0
    for item_list in items:
        count = len(item_list)
        if current_batch and current_count + count > max_items:
            batches.append(current_batch)
            current_batch = [item_list]
            current_count = count
        else:
            current_batch.append(item_list)
            current_count += count
    if current_batch:
        batches.append(current_batch)
    return batches
