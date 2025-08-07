"""
CLI клиент для Chat Processing API
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncGenerator

import httpx
import typer
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.json import JSON
from pydantic import BaseModel, ValidationError

# Initialize console and app
console = Console()
app = typer.Typer(help="CLI клиент для Chat Processing API")

# Configuration
DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_TIMEOUT = 300.0  # 5 минут
STREAM_TIMEOUT = 600.0   # 10 минут для стриминга
MAX_RETRIES = 3
RETRY_DELAY = 2.0

class ChatMessage(BaseModel):
    """Структура данных сообщения чата"""
    sender: str
    timestamp: str
    text: str

class ChatProcessingRequest(BaseModel):
    """Запрос на обработку чата"""
    chat_name: str
    messages: List[ChatMessage]

class QueryRequest(BaseModel):
    """Запрос на поиск по обработанному чату"""
    request_id: str
    query: str

class APIClient:
    """HTTP клиент для взаимодействия с API с улучшенной обработкой соединений"""
    
    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: float = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=10.0,
                read=timeout,
                write=10.0,
                pool=10.0
            ),
            limits=httpx.Limits(
                max_keepalive_connections=10,
                max_connections=20,
                keepalive_expiry=30.0
            ),
            follow_redirects=True
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def _retry_request(self, request_func, *args, **kwargs):
        """Выполнение запроса с повторными попытками"""
        last_exception = None
        
        for attempt in range(MAX_RETRIES):
            try:
                return await request_func(*args, **kwargs)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    console.print(f"[yellow]Попытка {attempt + 1} не удалась: {e}. Повторяю через {RETRY_DELAY} сек...[/yellow]")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))  # Экспоненциальная задержка
                else:
                    console.print(f"[red]Все попытки исчерпаны. Последняя ошибка: {e}[/red]")
            except httpx.HTTPStatusError as e:
                # HTTP ошибки не повторяем
                raise e
        
        raise last_exception
    
    async def health_check(self) -> Dict[str, Any]:
        """Проверка здоровья сервиса"""
        async def _request():
            response = await self.client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def get_api_info(self) -> Dict[str, Any]:
        """Получение информации об API"""
        async def _request():
            response = await self.client.get(f"{self.base_url}/")
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def process_chat(self, request: ChatProcessingRequest) -> Dict[str, str]:
        """Запуск обработки чата"""
        async def _request():
            response = await self.client.post(
                f"{self.base_url}/api/v1/chat/process",
                json=request.model_dump()
            )
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def get_status(self, request_id: str) -> Dict[str, Any]:
        """Получение статуса обработки"""
        async def _request():
            response = await self.client.get(
                f"{self.base_url}/api/v1/chat/process/{request_id}/status"
            )
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def list_requests(self) -> Dict[str, Any]:
        """Получение списка активных запросов"""
        async def _request():
            response = await self.client.get(f"{self.base_url}/api/v1/chat/requests")
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def query_chat(self, request: QueryRequest) -> Dict[str, Any]:
        """Поиск по обработанному чату"""
        async def _request():
            response = await self.client.post(
                f"{self.base_url}/api/v1/chat/query",
                json=request.model_dump()
            )
            response.raise_for_status()
            return response.json()
        
        return await self._retry_request(_request)
    
    async def stream_results(self, request_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Получение потока результатов обработки с переподключением"""
        max_stream_retries = 5
        retry_delay = 1.0
        
        for stream_attempt in range(max_stream_retries):
            try:
                console.print(f"[blue]Подключение к потоку (попытка {stream_attempt + 1})...[/blue]")
                
                async with self.client.stream(
                    'GET',
                    f"{self.base_url}/api/v1/chat/process/{request_id}/stream",
                    headers={"Accept": "text/event-stream"},
                    timeout=STREAM_TIMEOUT
                ) as response:
                    response.raise_for_status()
                    
                    async for line in response.aiter_lines():
                        if line.startswith('data: '):
                            data = line[6:]  # Remove 'data: ' prefix
                            if data.strip():
                                try:
                                    result = json.loads(data)
                                    yield result
                                    
                                    # Если получили финальный статус, выходим
                                    if result.get("status") in ["completed", "failed"]:
                                        return
                                        
                                except json.JSONDecodeError:
                                    console.print(f"[red]Ошибка парсинга JSON: {data}[/red]")
                    
                    # Если дошли до конца потока без финального статуса
                    console.print("[yellow]Поток завершился без финального статуса[/yellow]")
                    return
                    
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                console.print(f"[yellow]Ошибка потока: {e}[/yellow]")
                
                if stream_attempt < max_stream_retries - 1:
                    console.print(f"[yellow]Переподключение через {retry_delay} сек...[/yellow]")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Экспоненциальная задержка
                    
                    # Проверяем статус перед переподключением
                    try:
                        status = await self.get_status(request_id)
                        if status.get("status") in ["completed", "failed"]:
                            console.print("[blue]Обработка завершена во время переподключения[/blue]")
                            yield status
                            return
                    except Exception as status_error:
                        console.print(f"[yellow]Не удалось проверить статус: {status_error}[/yellow]")
                else:
                    console.print("[red]Все попытки подключения к потоку исчерпаны[/red]")
                    # Пытаемся получить финальный статус
                    try:
                        final_status = await self.get_status(request_id)
                        yield final_status
                    except Exception as final_error:
                        console.print(f"[red]Не удалось получить финальный статус: {final_error}[/red]")
                    return
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    console.print(f"[red]Запрос с ID {request_id} не найден[/red]")
                else:
                    console.print(f"[red]HTTP ошибка потока: {e.response.status_code}[/red]")
                return
            except Exception as e:
                console.print(f"[red]Неожиданная ошибка потока: {e}[/red]")
                return

def load_chat_from_file(file_path: Path) -> ChatProcessingRequest:
    """Загрузка чата из JSON файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return ChatProcessingRequest(**data)
    except FileNotFoundError:
        console.print(f"[red]Файл не найден: {file_path}[/red]")
        raise typer.Exit(1)
    except json.JSONDecodeError as e:
        console.print(f"[red]Ошибка парсинга JSON: {e}[/red]")
        raise typer.Exit(1)
    except ValidationError as e:
        console.print(f"[red]Ошибка валидации данных: {e}[/red]")
        raise typer.Exit(1)

def create_example_chat_file(file_path: Path):
    """Создание примера файла чата"""
    example_chat = {
        "chat_name": "Пример рабочего чата",
        "messages": [
            {
                "sender": "Алиса",
                "timestamp": "2025-01-15T10:00:00Z",
                "text": "Привет всем! Как дела с проектом?"
            },
            {
                "sender": "Боб",
                "timestamp": "2025-01-15T10:01:00Z",
                "text": "Все идет по плану, завтра презентация"
            },
            {
                "sender": "Алиса",
                "timestamp": "2025-01-15T10:02:00Z",
                "text": "Отлично! А что с документацией?"
            },
            {
                "sender": "Карл",
                "timestamp": "2025-01-15T10:03:00Z",
                "text": "Документация почти готова, осталось добавить примеры API"
            },
            {
                "sender": "Боб",
                "timestamp": "2025-01-15T10:04:00Z",
                "text": "Супер! Тогда мы успеваем в срок"
            }
        ]
    }
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(example_chat, f, ensure_ascii=False, indent=2)
    
    console.print(f"[green]Пример чата создан: {file_path}[/green]")

@app.command()
def health(
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    timeout: float = typer.Option(30.0, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Проверка здоровья сервиса"""
    async def _health():
        try:
            async with APIClient(base_url, timeout) as client:
                health_data = await client.health_check()
                
                # Create status table
                table = Table(title="Health Check")
                table.add_column("Parameter", style="cyan")
                table.add_column("Value", style="green")
                
                table.add_row("Status", health_data.get("status", "unknown"))
                table.add_row("Kafka Connected", str(health_data.get("kafka_connected", False)))
                table.add_row("Timestamp", health_data.get("timestamp", "unknown"))
                
                console.print(table)
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_health())

@app.command()
def info(
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    timeout: float = typer.Option(30.0, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Получение информации об API"""
    async def _info():
        try:
            async with APIClient(base_url, timeout) as client:
                api_info = await client.get_api_info()
                console.print(JSON(json.dumps(api_info, ensure_ascii=False)))
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_info())

@app.command()
def process(
    file_path: Path = typer.Argument(..., help="Путь к JSON файлу с чатом"),
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    stream: bool = typer.Option(True, "--stream/--no-stream", help="Показывать поток обработки"),
    save_result: Optional[Path] = typer.Option(None, "--save", "-s", help="Сохранить результат в файл"),
    timeout: float = typer.Option(DEFAULT_TIMEOUT, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Обработка чата из файла"""
    async def _process():
        # Load chat data
        chat_request = load_chat_from_file(file_path)
        
        try:
            async with APIClient(base_url, timeout) as client:
                # Start processing
                console.print(f"[blue]Запуск обработки чата: {chat_request.chat_name}[/blue]")
                start_response = await client.process_chat(chat_request)
                request_id = start_response["request_id"]
                
                console.print(f"[green]Обработка запущена! Request ID: {request_id}[/green]")
                
                if stream:
                    # Stream results
                    console.print("[blue]Получение результатов...[/blue]")
                    
                    last_result = None
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        console=console
                    ) as progress:
                        task = progress.add_task("Обработка...", total=None)
                        
                        async for result in client.stream_results(request_id):
                            last_result = result
                            status = result.get("status", "processing")
                            stage = result.get("stage", "unknown")
                            
                            progress.update(task, description=f"Статус: {status} | Этап: {stage}")
                            
                            # Show updates
                            if status == "completed":
                                progress.update(task, description="Завершено!")
                                console.print("[green]Обработка завершена успешно![/green]")
                                break
                            elif status == "failed":
                                progress.update(task, description="Ошибка!")
                                console.print(f"[red]Ошибка обработки: {result.get('error', 'Unknown error')}[/red]")
                                break
                    
                    # Show final result
                    if last_result:
                        console.print("\n[bold]Финальный результат:[/bold]")
                        console.print(JSON(json.dumps(last_result, ensure_ascii=False)))
                        
                        # Save result if requested
                        if save_result and last_result.get("status") == "completed":
                            with open(save_result, 'w', encoding='utf-8') as f:
                                json.dump(last_result, f, ensure_ascii=False, indent=2)
                            console.print(f"[green]Результат сохранен в: {save_result}[/green]")
                
                else:
                    console.print(f"[yellow]Используйте команду 'status {request_id}' для отслеживания прогресса[/yellow]")
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_process())

@app.command()
def status(
    request_id: str = typer.Argument(..., help="ID запроса"),
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    timeout: float = typer.Option(30.0, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Получение статуса обработки запроса"""
    async def _status():
        try:
            async with APIClient(base_url, timeout) as client:
                status_data = await client.get_status(request_id)
                
                # Create status table
                table = Table(title=f"Status для запроса {request_id}")
                table.add_column("Parameter", style="cyan")
                table.add_column("Value", style="green")
                
                for key, value in status_data.items():
                    table.add_row(key, str(value))
                
                console.print(table)
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                console.print(f"[red]Запрос с ID {request_id} не найден[/red]")
            else:
                console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_status())

@app.command()
def list_requests(
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    timeout: float = typer.Option(30.0, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Список активных запросов"""
    async def _list():
        try:
            async with APIClient(base_url, timeout) as client:
                requests_data = await client.list_requests()
                
                active_requests = requests_data.get("active_requests", [])
                count = requests_data.get("count", 0)
                
                if count == 0:
                    console.print("[yellow]Нет активных запросов[/yellow]")
                    return
                
                # Create requests table
                table = Table(title=f"Активные запросы ({count})")
                table.add_column("Request ID", style="cyan")
                table.add_column("Chat Name", style="green")
                table.add_column("Status", style="yellow")
                table.add_column("Messages", style="blue")
                table.add_column("Started At", style="magenta")
                
                for req in active_requests:
                    table.add_row(
                        req.get("request_id", "unknown"),
                        req.get("chat_name", "unknown"),
                        req.get("status", "unknown"),
                        str(req.get("message_count", 0)),
                        req.get("started_at", "unknown")
                    )
                
                console.print(table)
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_list())

@app.command()
def create_example(
    file_path: Path = typer.Argument("example_chat.json", help="Путь для создания примера")
):
    """Создание примера файла чата"""
    if file_path.exists():
        overwrite = typer.confirm(f"Файл {file_path} уже существует. Перезаписать?")
        if not overwrite:
            console.print("[yellow]Отменено[/yellow]")
            return
    
    create_example_chat_file(file_path)

def create_sample_messages() -> List[Dict[str, str]]:
    """Create sample chat messages for demo"""
    return [
        # Обсуждение технологий
        {
            "sender": "Alice",
            "timestamp": "2024-01-01T10:00:00Z",
            "text": "Кто-нибудь пробовал новый ChatGPT? Он невероятно улучшился!"
        },
        {
            "sender": "Bob",
            "timestamp": "2024-01-01T10:01:00Z",
            "text": "Да, я использую его для программирования. Очень помогает с отладкой кода."
        },
        {
            "sender": "Charlie",
            "timestamp": "2024-01-01T10:02:00Z",
            "text": "А я опасаюсь, что ИИ заменит программистов. Что думаете?"
        },
        {
            "sender": "Alice",
            "timestamp": "2024-01-01T10:03:00Z",
            "text": "Думаю, ИИ скорее поможет нам стать более продуктивными, чем заменит."
        },
        {
            "sender": "Dave",
            "timestamp": "2024-01-01T10:04:00Z",
            "text": "Согласен с Alice. Нужно учиться работать с ИИ, а не бояться его."
        },
        # Планирование встречи
        {
            "sender": "Eve",
            "timestamp": "2024-01-01T11:00:00Z",
            "text": "Когда планируем встречу по проекту? У меня свободно во вторник и четверг."
        },
        {
            "sender": "Frank",
            "timestamp": "2024-01-01T11:01:00Z",
            "text": "Четверг мне подходит. В какое время удобно всем?"
        },
        {
            "sender": "Grace",
            "timestamp": "2024-01-01T11:02:00Z",
            "text": "Предлагаю 14:00, чтобы все успели пообедать."
        },
        {
            "sender": "Eve",
            "timestamp": "2024-01-01T11:03:00Z",
            "text": "Отлично! Четверг в 14:00. Создам календарное приглашение."
        },
        # Обсуждение экологии
        {
            "sender": "Helen",
            "timestamp": "2024-01-01T12:00:00Z",
            "text": "Читала статью о глобальном потеплении. Ситуация критическая!"
        },
        {
            "sender": "Ivan",
            "timestamp": "2024-01-01T12:01:00Z",
            "text": "Да, нужно срочно переходить на возобновляемые источники энергии."
        },
        {
            "sender": "Jack",
            "timestamp": "2024-01-01T12:02:00Z",
            "text": "Солнечные панели становятся все дешевле. Это обнадеживает."
        },
        {
            "sender": "Kate",
            "timestamp": "2024-01-01T12:03:00Z",
            "text": "В нашем городе установили ветряки. Покрывают 30% энергопотребления."
        },
        {
            "sender": "Helen",
            "timestamp": "2024-01-01T12:04:00Z",
            "text": "Каждый может внести вклад: меньше пластика, больше переработки."
        },
        {
            "sender": "Ivan",
            "timestamp": "2024-01-01T12:05:00Z",
            "text": "И электромобили! Я планирую купить Tesla в этом году."
        },
        # Обсуждение еды
        {
            "sender": "Liam",
            "timestamp": "2024-01-01T13:00:00Z",
            "text": "Кто знает хороший рецепт борща? Хочу приготовить на выходных."
        },
        {
            "sender": "Mia",
            "timestamp": "2024-01-01T13:01:00Z",
            "text": "У меня есть семейный рецепт! Главное - хорошая свекла и долгое томление."
        },
        {
            "sender": "Noah",
            "timestamp": "2024-01-01T13:02:00Z",
            "text": "А я добавляю немного копченого мяса для аромата."
        },
        {
            "sender": "Olivia",
            "timestamp": "2024-01-01T13:03:00Z",
            "text": "Не забудьте сметану! Без неё борщ не борщ 😊"
        },
        # Случайные сообщения
        {
            "sender": "Paul",
            "timestamp": "2024-01-01T14:00:00Z",
            "text": "Погода сегодня отличная! Идеально для прогулки."
        },
        {
            "sender": "Quinn",
            "timestamp": "2024-01-01T14:01:00Z",
            "text": "Согласен! Я пойду в парк с собакой."
        },
        {
            "sender": "Rachel",
            "timestamp": "2024-01-01T15:00:00Z",
            "text": "Кто смотрел последний эпизод сериала? Какой поворот!"
        },
        {
            "sender": "Sam",
            "timestamp": "2024-01-01T15:01:00Z",
            "text": "Не спойлери! Я ещё не посмотрел 😅"
        }
    ]

@app.command()
def demo(
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    stream: bool = typer.Option(True, "--stream/--no-stream", help="Показывать поток обработки"),
    save_result: Optional[Path] = typer.Option(None, "--save", "-s", help="Сохранить результат в файл"),
    timeout: float = typer.Option(DEFAULT_TIMEOUT, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Демо с примерными данными"""
    async def _demo():
        # Load chat data
        chat_request = ChatProcessingRequest(chat_name="Demo Chat", messages=create_sample_messages())
        try:
            async with APIClient(base_url, timeout) as client:
                # Start processing
                console.print(f"[blue]Запуск обработки чата: {chat_request.chat_name}[/blue]")
                start_response = await client.process_chat(chat_request)
                request_id = start_response["request_id"]
                
                console.print(f"[green]Обработка запущена! Request ID: {request_id}[/green]")
                
                if stream:
                    # Stream results
                    console.print("[blue]Получение результатов...[/blue]")
                    
                    last_result = None
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        console=console
                    ) as progress:
                        task = progress.add_task("Обработка...", total=None)
                        
                        async for result in client.stream_results(request_id):
                            last_result = result
                            status = result.get("status", "processing")
                            stage = result.get("stage", "unknown")
                            
                            progress.update(task, description=f"Статус: {status} | Этап: {stage}")
                            
                            # Show updates
                            if status == "completed":
                                progress.update(task, description="Завершено!")
                                console.print("[green]Обработка завершена успешно![/green]")
                                break
                            elif status == "failed":
                                progress.update(task, description="Ошибка!")
                                console.print(f"[red]Ошибка обработки: {result.get('error', 'Unknown error')}[/red]")
                                break
                    
                    # Show final result
                    if last_result:
                        console.print("\n[bold]Финальный результат:[/bold]")
                        console.print(JSON(json.dumps(last_result, ensure_ascii=False)))
                        
                        # Save result if requested
                        if save_result and last_result.get("status") == "completed":
                            with open(save_result, 'w', encoding='utf-8') as f:
                                json.dump(last_result, f, ensure_ascii=False, indent=2)
                            console.print(f"[green]Результат сохранен в: {save_result}[/green]")
                
                else:
                    console.print(f"[yellow]Используйте команду 'status {request_id}' для отслеживания прогресса[/yellow]")
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
            raise typer.Exit(1)
    
    asyncio.run(_demo())


@app.command()
def query(
    request_id: str = typer.Argument(..., help="ID запроса обработки чата"),
    query_text: str = typer.Argument(..., help="Поисковый запрос"),
    base_url: str = typer.Option(DEFAULT_BASE_URL, "--url", "-u", help="Base URL сервера"),
    save_result: Optional[Path] = typer.Option(None, "--save", "-s", help="Сохранить результат в файл"),
    timeout: float = typer.Option(60.0, "--timeout", "-t", help="Таймаут запроса в секундах")
):
    """Поиск по обработанному чату"""
    async def _query():
        try:
            async with APIClient(base_url, timeout) as client:
                # Create query request
                query_request = QueryRequest(request_id=request_id, query=query_text)
                
                console.print(f"[blue]Поиск по запросу '{query_text}' в чате {request_id}[/blue]")
                
                # Execute query
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task("Выполняется поиск...", total=None)
                    
                    query_result = await client.query_chat(query_request)
                    
                    progress.update(task, description="Поиск завершен!")
                
                # Display results
                console.print("[green]Результаты поиска:[/green]")
                
                # Show query info
                console.print(f"[bold]Запрос:[/bold] {query_result.get('query', 'unknown')}")
                console.print(f"[bold]Request ID:[/bold] {query_result.get('request_id', 'unknown')}")
                
                # Show answer if available
                if 'answer' in query_result:
                    console.print(f"\n[bold green]Ответ:[/bold green]")
                    console.print(Panel(query_result['answer'], title="Ответ системы", border_style="green"))
                
               
                # Show full JSON if requested
                console.print(f"\n[bold]Полный результат:[/bold]")
                console.print(JSON(json.dumps(query_result, ensure_ascii=False)))
                
                # Save result if requested
                if save_result:
                    with open(save_result, 'w', encoding='utf-8') as f:
                        json.dump(query_result, f, ensure_ascii=False, indent=2)
                    console.print(f"[green]Результат сохранен в: {save_result}[/green]")
                
        except httpx.RequestError as e:
            console.print(f"[red]Ошибка подключения: {e}[/red]")
            raise typer.Exit(1)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                console.print(f"[red]Запрос с ID {request_id} не найден или не завершен[/red]")
            else:
                console.print(f"[red]HTTP ошибка: {e.response.status_code}[/red]")
                # Try to show error details
                try:
                    error_detail = e.response.json()
                    console.print(f"[red]Детали ошибки: {error_detail.get('detail', 'unknown')}[/red]")
                except:
                    pass
            raise typer.Exit(1)
    
    asyncio.run(_query())



if __name__ == "__main__":
    app()