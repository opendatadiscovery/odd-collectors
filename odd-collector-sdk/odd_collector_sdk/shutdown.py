import asyncio

from .logger import logger


async def shutdown_by(signal, loop: asyncio.AbstractEventLoop):
    """Cleanup tasks tied to the service's shutdown. By signal."""
    logger.info(f"Received exit signal {signal.name}")
    await shutdown(loop)


async def shutdown(loop: asyncio.AbstractEventLoop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info("Calling shutdown")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]
    logger.info(f"Canceling outstanding {len(tasks)} tasks")
    await asyncio.gather(*tasks)
    loop.stop()
    logger.info("Shutdown complete")
