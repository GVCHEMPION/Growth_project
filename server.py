"""
FastAPI сервер для взаимодействия с микросервисом обработки чатов через Kafka
"""

import os
import sys
import asyncio
import json
import logging
import uuid
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pythonjsonlogger import jsonlogger

from models import ChatMessage, ChatProcessingRequest, ProcessingStatus, TopicSummary, ProcessingResult, QueryRequest, QueryResponse

# def setup_logging():
#     """Настройка логирования для контейнера"""
#     # Убираем все существующие обработчики
#     root_logger = logging.getLogger()
#     for handler in root_logger.handlers[:]:
#         root_logger.removeHandler(handler)
    
#     # Создаем форматтер
#     formatter = jsonlogger.JsonFormatter(
#         '%(asctime)s %(name)s %(levelname)s %(message)s',
#         json_ensure_ascii=False
#     )
    
#     # Создаем обработчик для stdout (это важно для Docker!)
#     stdout_handler = logging.StreamHandler(sys.stdout)
#     stdout_handler.setFormatter(formatter)
#     stdout_handler.setLevel(logging.INFO)
    
#     # Настраиваем root logger
#     root_logger.setLevel(logging.INFO)
#     root_logger.addHandler(stdout_handler)
    
#     # Настраиваем логгер приложения
#     app_logger = logging.getLogger(__name__)
#     app_logger.setLevel(logging.INFO)
    
#     # Отключаем буферизацию для немедленного вывода логов
#     sys.stdout.flush()
#     sys.stderr.flush()
    
#     return app_logger

# # Настраиваем логирование ДО создания логгера
# logger = setup_logging()
logger = logging.getLogger(__name__)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(json_ensure_ascii=False)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)


# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = "chat_input_topic"
OUTPUT_TOPIC = "chat_output_topic"

class KafkaManager:
    """Менеджер Kafka соединений"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.response_handlers: Dict[str, asyncio.Queue] = {}
        self.consumer_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Инициализация Kafka соединений"""
        logger.info("Starting Kafka manager...")
        
        # Initialize producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            retry_backoff_ms=1000,
            max_batch_size=16384
        )
        await self.producer.start()
        
        # Initialize consumer
        self.consumer = AIOKafkaConsumer(
            OUTPUT_TOPIC,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        await self.consumer.start()
        
        # Start consumer task
        self.consumer_task = asyncio.create_task(self._consume_responses())
        
        logger.info("Kafka manager started")
    
    async def stop(self):
        """Остановка Kafka соединений"""
        logger.info("Stopping Kafka manager...")
        
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
        
        if self.producer:
            await self.producer.stop()
        
        logger.info("Kafka manager stopped")
    
    async def _consume_responses(self):
        """Обработка ответов от микросервиса"""
        try:
            async for message in self.consumer:
                response_data = message.value
                request_id = response_data.get("request_id")
                
                if request_id and request_id in self.response_handlers:
                    queue = self.response_handlers[request_id]
                    await queue.put(response_data)
                    
                    # If processing is completed or failed, mark it as done
                    status = response_data.get("status", "")
                    if status in ["completed", "failed"]:
                        await queue.put(None)  # Signal end
                else:
                    logger.warning(f"Received response for unknown request_id: {request_id}")
        
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
        except Exception as e:
            logger.error(f"Error in consumer task: {e}")
    
    async def send_request(self, request_data: Dict[str, Any]) -> str:
        """Отправка запроса в Kafka"""
        # if request_id is None:
        request_id = str(uuid.uuid4())
        request_data["request_id"] = request_id
        
        # Create response queue for this request
        self.response_handlers[request_id] = asyncio.Queue()
        
        try:
            await self.producer.send_and_wait(INPUT_TOPIC, request_data)
            print(f"Sent request {request_id} to Kafka")
            return request_id
        except Exception as e:
            # Clean up on error
            if request_id in self.response_handlers:
                del self.response_handlers[request_id]
            raise e
    
    async def get_response_stream(self, request_id: str):
        """Получение потока ответов для запроса"""
        if request_id not in self.response_handlers:
            raise ValueError(f"No handler for request_id: {request_id}")
        
        queue = self.response_handlers[request_id]
        
        try:
            while True:
                response = await asyncio.wait_for(queue.get(), timeout=600.0)  # 10 min timeout
                
                if response is None:  # End signal
                    break
                    
                yield response
        
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for response for request_id: {request_id}")
            yield {
                "request_id": request_id,
                "error": "Request timeout",
                "status": "failed",
                "stage": "timeout",
                "timestamp": datetime.now().isoformat()
            }
        
        finally:
            # Clean up
            if request_id in self.response_handlers:
                del self.response_handlers[request_id]

class ChatProcessingService:
    """Сервис обработки чатов"""
    
    def __init__(self, kafka_manager: KafkaManager):
        self.kafka_manager = kafka_manager
        self.active_requests: Dict[str, Dict[str, Any]] = {}

    async def query_chat(self, request: QueryRequest) -> Dict[str, Any]:
        """Выполнение поиска по обработанному чату"""
        # Создаем данные для отправки в Kafka
        query_data = {
            "text_request_id": request.request_id,
            "query": request.query,
            "type": "rag_search",
            "timestamp": datetime.now().isoformat()
        }
        
        # Отправляем запрос в Kafka
        search_request_id = await self.kafka_manager.send_request(query_data)
        
        # Ждем ответ от микросервиса
        response_data = None
        async for response in self.kafka_manager.get_response_stream(search_request_id):
            response_data = response
            if response_data["status"] in ("completed", "failed"):
                break

        logger.info(f"Received response from processing service: {response_data}")
        if not response_data:
            raise HTTPException(status_code=500, detail="No response from processing service")
        
        return {
            "request_id": request.request_id,
            "query": request.query,
            "status": response_data.get("status", "falied"),
            "answer": response_data.get("answer", ""),
            "timestamp": response_data.get("timestamp", datetime.now().isoformat())
        }
        # if response_data.get("status") == "failed":
        #     return 
        #     # raise HTTPException(status_code=500, detail=f"Query processing failed: {error_message}")
        
        # return {
        #     "request_id": request.request_id,
        #     "query": request.query,
        #     "message": response_data.get("answer", ""),
        #     "timestamp": response_data.get("timestamp", datetime.now().isoformat())
        # }
    
    async def process_chat(self, request: ChatProcessingRequest) -> str:
        """Запуск обработки чата"""
        # Convert request to dict
        request_data = {
            "chat_name": request.chat_name,
            "messages": [msg.model_dump() for msg in request.messages],
            "timestamp": datetime.now().isoformat()
        }
        
        # Send to Kafka
        request_id = await self.kafka_manager.send_request(request_data)
        
        # Store request info
        self.active_requests[request_id] = {
            "chat_name": request.chat_name,
            "message_count": len(request.messages),
            "started_at": datetime.now().isoformat(),
            "status": "processing"
        }
        
        logger.info(f"Started processing chat '{request.chat_name}' with request_id: {request_id}")
        return request_id
    
    async def get_processing_stream(self, request_id: str):
        """Получение потока обработки чата"""
        if request_id not in self.active_requests:
            raise HTTPException(status_code=404, detail="Request not found")
        
        async for response in self.kafka_manager.get_response_stream(request_id):
            # Update request status
            if request_id in self.active_requests:
                self.active_requests[request_id]["status"] = response.get("status", "processing")
                if response.get("status") in ["completed", "failed"]:
                    self.active_requests[request_id]["completed_at"] = datetime.now().isoformat()
            
            yield f"data: {json.dumps(response, ensure_ascii=False)}\n\n"
        
        # Clean up completed request
        if request_id in self.active_requests:
            del self.active_requests[request_id]
    
    def get_request_status(self, request_id: str) -> Dict[str, Any]:
        """Получение статуса запроса"""
        if request_id not in self.active_requests:
            raise HTTPException(status_code=404, detail="Request not found")
        
        return self.active_requests[request_id]
    
    def list_active_requests(self) -> List[Dict[str, Any]]:
        """Получение списка активных запросов"""
        return [
            {"request_id": req_id, **req_data}
            for req_id, req_data in self.active_requests.items()
        ]

# Global instances
kafka_manager = KafkaManager(KAFKA_BOOTSTRAP_SERVERS)
chat_service = ChatProcessingService(kafka_manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    await kafka_manager.start()
    yield
    # Shutdown
    await kafka_manager.stop()

# Create FastAPI app
app = FastAPI(
    title="Chat Processing API",
    description="API для обработки чатов с выделением тем и генерацией саммари",
    version="1.0.0",
    lifespan=lifespan
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    client_ip = request.client.host
    method = request.method
    path = request.url.path
    query_params = request.query_params
    
    logger.debug(
        f"Incoming request: {method} {path} from {client_ip} "
        f"with query params: {query_params if query_params else 'none'}"
    )
    
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        logger.debug(
            f"Completed request: {method} {path} from {client_ip} "
            f"status: {response.status_code} "
            f"processing time: {process_time:.2f}ms"
        )
        return response
    except Exception as e:
        logger.error(f"Error processing request: {method} {path} from {client_ip}: {str(e)}", exc_info=True)
        raise

@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "message": "Chat Processing API",
        "version": "1.0.0",
        "endpoints": {
            "process_chat": "/api/v1/chat/process",
            "stream_processing": "/api/v1/chat/process/{request_id}/stream",
            "get_status": "/api/v1/chat/process/{request_id}/status",
            "list_requests": "/api/v1/chat/requests",
            "query_chat": "/api/v1/chat/query"
        }
    }

@app.post("/api/v1/chat/process", response_model=Dict[str, str])
async def process_chat(request: ChatProcessingRequest):
    """
    Запуск обработки чата
    
    Отправляет чат на обработку в микросервис через Kafka.
    Возвращает request_id для отслеживания прогресса.
    """
    print("Processing chat...")
    try:
        request_id = await chat_service.process_chat(request)
        return {
            "request_id": request_id,
            "message": "Chat processing started",
            "stream_url": f"/api/v1/chat/process/{request_id}/stream"
        }
    except Exception as e:
        logger.error(f"Error starting chat processing: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start processing: {str(e)}")

@app.get("/api/v1/chat/process/{request_id}/stream")
async def stream_processing_results(request_id: str):
    """
    Получение потока результатов обработки чата
    
    Возвращает Server-Sent Events с прогрессом обработки и результатами.
    """
    try:
        return StreamingResponse(
            chat_service.get_processing_stream(request_id),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Cache-Control"
            }
        )
    except Exception as e:
        logger.error(f"Error streaming results for {request_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stream results: {str(e)}")

@app.get("/api/v1/chat/process/{request_id}/status")
async def get_processing_status(request_id: str):
    """
    Получение текущего статуса обработки запроса
    """
    try:
        status = chat_service.get_request_status(request_id)
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting status for {request_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")

@app.get("/api/v1/chat/requests")
async def list_active_requests():
    """
    Получение списка всех активных запросов на обработку
    """
    try:
        requests = chat_service.list_active_requests()
        return {
            "active_requests": requests,
            "count": len(requests)
        }
    except Exception as e:
        logger.error(f"Error listing requests: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list requests: {str(e)}")

# class QueryRequest(BaseModel):
#     """Запрос на поиск по обработанному чату"""
#     request_id: str = Field(..., description="ID запроса обработки чата")
#     query: str = Field(..., description="Поисковый запрос")    
@app.post("/api/v1/chat/query", response_model=QueryResponse)
async def query_chat(request: QueryRequest):
    """
    Поиск по обработанному чату
    
    Выполняет RAG-поиск по результатам обработки чата.
    Требует request_id от предыдущей обработки чата.
    """
    logger.warning(f"Processing query for {request.request_id}: '{request.query}'")
    try:
        result = await chat_service.query_chat(request)
        return QueryResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing query for {request.request_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process query: {str(e)}")

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    logger.info("Checking health...")
    print("Checking health...")
    return {
        "status": "healthy",
        "kafka_connected": kafka_manager.producer is not None and kafka_manager.consumer is not None,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )