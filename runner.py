import asyncio
import asyncssh


async def run_command(host, port, username, password, command, raise_error=False):
    async with asyncssh.connect(host=host, port=port, username=username, password=password) as connection:
        result = await connection.run(command)
        print(result)


if __name__=="__main__":
    asyncio.get_event_loop().run_until_complete(run_command(
        "10.148.128.236",
        22,
        "tts",
        "tts1234",
        "sudo systemctl status cassandra"
    ))
