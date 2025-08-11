"""
Microservice for processing chat messages
"""

import asyncio
import json
import logging
import os
import time
import re
import inspect
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Any, AsyncGenerator, Tuple, Literal, Optional
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import httpx
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from pythonjsonlogger import jsonlogger
from sklearn.cluster import KMeans
from umap import UMAP
from hdbscan import HDBSCAN
from redisvl.index import AsyncSearchIndex
from redisvl.query import VectorQuery, HybridQuery, CountQuery, FilterQuery
from redisvl.query.filter import Tag
from redisvl.schema import IndexSchema
from redisvl.utils.vectorize import CustomTextVectorizer
from redisvl.redis.utils import array_to_buffer
from redisvl.extensions.cache.llm import SemanticCache
from redisvl.extensions.message_history import MessageHistory

from models import ChatMessage, Safe_Query_Schema, Enough_Query_Schema, Requering_schema, Answer_Schema, Deep_Logic_Schema
# class ChatMessage(BaseModel): #используется pydantic
#     """Структура данных сообщения чата"""
#     sender: str = Field(..., description="Отправитель/автор сообщения")
#     timestamp: str = Field(..., description="Временная метка сообщения")
#     text: str = Field(..., description="Текстовое содержимое сообщения")

# Configure logging
logger = logging.getLogger(__name__)

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(json_ensure_ascii=False)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Configuration - Updated topics to match chat server
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = "chat_input_topic"
OUTPUT_TOPIC = "chat_output_topic"
VLLM_URL = os.getenv('CHAT_SERVER_URL', "http://ollama:11434")
REDIS_URL = os.getenv('REDIS_URL', "redis://redis:6379")
TOPICS_DIR = "./chat_topics"

# Fixed embedding model options
EMBEDDING_MODEL = "deepvk/USER-bge-m3"
USE_TRUST_REMOTE_CODE = True
SUMMARY_MODEL = "gemma3:12b-it-qat"#"gemma-3-12b-it"
START_TOKEN = "<start_of_turn>"
END_TOKEN = "<end_of_turn>"

# Ensure topics directory exists
os.makedirs(TOPICS_DIR, exist_ok=True)

class TextGenerator:
    """Separate class for text generation using vLLM"""
    
    def __init__(self, vllm_url: str, model_name: str, end_token: str):
        self.vllm_url = vllm_url
        self.model_name = model_name
        self.end_token = end_token
        self.client = httpx.AsyncClient(timeout=600.0)
    
    async def generate_text(self, system_prompt: str, prompt_1: str, prompt_2: str = "", max_tokens: int = 16384, temperature: float = 0.15, 
                           top_p: float = 0.8, stop_sequences: List[str] = None) -> str:
        """Generate text using vLLM API with retry logic"""
        try:
            if stop_sequences is None:
                stop_sequences = [self.end_token]
            
            # Format prompt with Gemma 3 12B IT tokens
            formatted_prompt = f"{START_TOKEN}system {system_prompt} {END_TOKEN}"
            formatted_prompt += f"{START_TOKEN}user {prompt_1}{END_TOKEN}{START_TOKEN}model {prompt_2}"
            # logger.info(formatted_prompt)
            payload = {
                "model": self.model_name,
                "prompt": formatted_prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "stop": stop_sequences
            }
            
            # Retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = await self.client.post(
                        f"{self.vllm_url}/v1/completions",
                        json=payload
                    )
                    # logger.info(response.text)
                    if response.status_code == 200:
                        result = response.json()
                        # logger.info(result)
                        generated_text = result["choices"][0]["text"].strip()
                        # generated_text = generated_text.encode().decode('utf-8')
                        # print(generated_text)
                        logger.info(f"Text generated successfully (attempt {attempt + 1})")
                        return generated_text
                    else:
                        logger.warning(f"vLLM API error on attempt {attempt + 1}: {response.status_code}")
                        if attempt == max_retries - 1:
                            return f"Ошибка генерации: HTTP {response.status_code}"
                        
                except httpx.TimeoutException:
                    logger.warning(f"Timeout on attempt {attempt + 1}")
                    if attempt == max_retries - 1:
                        return "Ошибка генерации: превышено время ожидания"
                    await asyncio.sleep(2 ** attempt)
                    
                except httpx.RemoteProtocolError as e:
                    logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return "Ошибка генерации: проблема соединения с сервером"
                    await asyncio.sleep(2 ** attempt)

                except Exception as e:
                    logger.error(f"Error during text generation on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return f"Ошибка генерации текста: {str(e)}"
                    await asyncio.sleep(2 ** attempt)
            
            return "Ошибка генерации: все попытки исчерпаны"
        
        except Exception as e:
            logger.error(f"Error during text generation: {e}")
            return f"Ошибка генерации текста: {str(e)}"
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

class BERTClusteringService:
    """Отдельный класс для работы с BERT и кластеризацией"""
    
    def __init__(self, embedding_model_name: str = EMBEDDING_MODEL, use_trust_remote_code: bool = USE_TRUST_REMOTE_CODE):
        self.embedding_model_name = embedding_model_name
        self.use_trust_remote_code = use_trust_remote_code
        self.embedding_model = None
        self.topic_model = None
        self.executor = ThreadPoolExecutor(max_workers=2)
    
    async def initialize(self):
        """Инициализация модели эмбеддингов"""
        logger.info("Initializing BERT clustering service...")
        
        # Инициализация модели эмбеддингов в thread pool
        loop = asyncio.get_event_loop()
        self.embedding_model = await loop.run_in_executor(
            self.executor,
            self._load_embedding_model
        )
        
        logger.info("BERT clustering service initialized")
    
    def _load_embedding_model(self):
        """Загрузка модели эмбеддингов (выполняется в thread pool)"""
        logger.info(f"Loading embedding model: {self.embedding_model_name}")
        
        try:
            model = SentenceTransformer(self.embedding_model_name, trust_remote_code=self.use_trust_remote_code)
            logger.info(f"Successfully loaded model: {self.embedding_model_name}")
            return model
            
        except Exception as e:
            logger.error(f"Failed to load {self.embedding_model_name}: {e}")
            # Fallback к более надежной модели
            fallback_model = "sentence-transformers/all-MiniLM-L6-v2"
            logger.info(f"Falling back to: {fallback_model}")
            try:
                model = SentenceTransformer(fallback_model)
                logger.info(f"Successfully loaded fallback model: {fallback_model}")
                return model
            except Exception as e2:
                logger.error(f"Failed to load fallback model: {e2}")
                raise e2
    
    def _create_adaptive_topic_model(self, texts: List[str]):
        """Создание адаптивной модели BERTopic на основе характеристик данных"""
        
        # Анализ данных для адаптивных параметров
        text_lengths = [len(text.split()) for text in texts]
        avg_length = sum(text_lengths) / len(text_lengths)
        
        # Анализ разнообразия текстов через простые метрики
        unique_words = set()
        for text in texts:
            unique_words.update(text.lower().split())
        
        diversity_ratio = len(unique_words) / len(texts)
        
        # Адаптивные параметры на основе характеристик данных
        if diversity_ratio > 10:  # Высокое разнообразие
            base_neighbors = max(3, int(len(texts) ** 0.3))
            base_cluster_size = max(2, int(len(texts) ** 0.25))
        elif diversity_ratio > 5:  # Среднее разнообразие
            base_neighbors = max(2, int(len(texts) ** 0.4))
            base_cluster_size = max(2, int(len(texts) ** 0.3))
        else:  # Низкое разнообразие
            base_neighbors = max(2, int(len(texts) ** 0.5))
            base_cluster_size = max(2, int(len(texts) ** 0.4))
        
        # Корректировка на основе средней длины текстов
        if avg_length > 20:  # Длинные тексты
            base_neighbors = min(base_neighbors + 2, len(texts) // 2)
            base_cluster_size = max(base_cluster_size, 3)
        elif avg_length < 5:  # Короткие тексты
            base_neighbors = max(base_neighbors - 1, 2)
            base_cluster_size = max(base_cluster_size - 1, 2)
        
        n_components = min(max(5, int(len(texts) ** 0.3)), 50)
        
        logger.info(f"Adaptive BERTopic parameters for {len(texts)} texts:")
        logger.info(f"  - Text diversity ratio: {diversity_ratio:.2f}")
        logger.info(f"  - Average text length: {avg_length:.1f}")
        logger.info(f"  - n_neighbors: {base_neighbors}")
        logger.info(f"  - min_cluster_size: {base_cluster_size}")
        
        umap_model = UMAP(
            n_neighbors=base_neighbors,
            n_components=n_components,
            min_dist=0.0,
            metric='cosine',
            random_state=42
        )
        
        hdbscan_model = HDBSCAN(
            min_cluster_size=base_cluster_size,
            min_samples=max(1, base_cluster_size // 2),
            metric='euclidean',
            cluster_selection_method='eom',
            prediction_data=True
        )
        
        self.topic_model = BERTopic(
            embedding_model=self.embedding_model,
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            verbose=True,
            calculate_probabilities=False,
            nr_topics=None
        )
        
        return self.topic_model
    
    async def run_bertopic_clustering(self, texts: List[str]):
        """Запуск кластеризации BERTopic (асинхронный интерфейс)"""
        try:
            loop = asyncio.get_event_loop()
            topics, probabilities = await loop.run_in_executor(
                self.executor,
                self._run_bertopic_clustering_sync,
                texts
            )
            return topics, probabilities
        except Exception as e:
            logger.error(f"BERTopic clustering failed: {e}")
            raise
    
    def _run_bertopic_clustering_sync(self, texts: List[str]):
        """Синхронная кластеризация BERTopic (для thread pool)"""
        try:
            # Создание адаптивной модели
            topic_model = self._create_adaptive_topic_model(texts)
            topics, probabilities = topic_model.fit_transform(texts)
            return topics, probabilities
        except Exception as e:
            logger.error(f"BERTopic clustering failed: {e}")
            raise
    
    async def run_adaptive_kmeans_clustering(self, texts: List[str]):
        """Адаптивная кластеризация K-means с автоматическим определением количества кластеров"""
        try:
            loop = asyncio.get_event_loop()
            topics, probabilities = await loop.run_in_executor(
                self.executor,
                self._run_adaptive_kmeans_clustering_sync,
                texts
            )
            return topics, probabilities
        except Exception as e:
            logger.error(f"Adaptive K-means clustering failed: {e}")
            raise
    
    def _run_adaptive_kmeans_clustering_sync(self, texts: List[str]):
        """Синхронная адаптивная K-means кластеризация"""
        try:
            # Генерация эмбеддингов
            embeddings = self.embedding_model.encode(texts)
            
            # Silhouette analysis для определения оптимального количества кластеров
            from sklearn.metrics import silhouette_score
            
            max_clusters = min(len(texts) // 2, 15)
            min_clusters = 2
            
            best_score = -1
            best_clusters = min_clusters
            
            for n_clusters in range(min_clusters, max_clusters + 1):
                try:
                    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10, n_jobs=1)
                    cluster_labels = kmeans.fit_predict(embeddings)
                    
                    if len(set(cluster_labels)) < 2:
                        continue
                        
                    score = silhouette_score(embeddings, cluster_labels)
                    
                    if score > best_score:
                        best_score = score
                        best_clusters = n_clusters
                        
                except Exception:
                    continue
            
            logger.info(f"Optimal clusters found: {best_clusters} (silhouette score: {best_score:.3f})")
            
            # Финальная кластеризация с оптимальным количеством
            kmeans = KMeans(n_clusters=best_clusters, random_state=42, n_init=10, n_jobs=1)
            cluster_labels = kmeans.fit_predict(embeddings)
            
            # Конвертация в BERTopic-подобный формат
            topics = cluster_labels.tolist()
            probabilities = [1.0] * len(topics)
            
            return topics, probabilities
            
        except Exception as e:
            logger.error(f"Adaptive K-means clustering failed: {e}")
            # Fallback: метод локтя
            return self._run_elbow_kmeans_clustering_sync(texts)
    
    def _run_elbow_kmeans_clustering_sync(self, texts: List[str]):
        """Fallback K-means с аппроксимацией метода локтя"""
        try:
            embeddings = self.embedding_model.encode(texts)
            
            # Простая аппроксимация elbow method
            max_clusters = min(len(texts) // 2, 10)
            min_clusters = 2
            
            inertias = []
            cluster_range = range(min_clusters, max_clusters + 1)
            
            for n_clusters in cluster_range:
                kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                kmeans.fit(embeddings)
                inertias.append(kmeans.inertia_)
            
            # Поиск "локтя" - точки наибольшего изменения градиента
            if len(inertias) >= 3:
                gradients = []
                for i in range(1, len(inertias)):
                    gradients.append(inertias[i-1] - inertias[i])
                
                # Поиск точки где градиент резко уменьшается
                best_idx = 0
                max_gradient_change = 0
                
                for i in range(1, len(gradients)):
                    gradient_change = gradients[i-1] - gradients[i]
                    if gradient_change > max_gradient_change:
                        max_gradient_change = gradient_change
                        best_idx = i
                
                optimal_clusters = min_clusters + best_idx
            else:
                optimal_clusters = min_clusters
            
            logger.info(f"Elbow method suggests {optimal_clusters} clusters")
            
            # Финальная кластеризация
            kmeans = KMeans(n_clusters=optimal_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(embeddings)
            
            topics = cluster_labels.tolist()
            probabilities = [1.0] * len(topics)
            
            return topics, probabilities
            
        except Exception as e:
            logger.error(f"Elbow method clustering failed: {e}")
            # Последний резерв: возвращаем все в одну группу
            return [0] * len(texts), [1.0] * len(texts)
    
    async def cluster_texts(self, texts: List[str]) -> Tuple[List[int], List[float]]:
        """Основной метод кластеризации с адаптивными стратегиями"""
        logger.info(f"Clustering {len(texts)} texts...")
        start_time = time.time()
        
        try:
            # Стратегия 1: Попытка BERTopic с динамическими параметрами
            try:
                topics, probabilities = await self.run_bertopic_clustering(texts)
                
                # Проверка, что BERTopic нашел значимые кластеры
                unique_topics = set(topics)
                if len(unique_topics) <= 1 and -1 in unique_topics:
                    logger.warning("BERTopic found too few clusters, trying alternative method...")
                    raise ValueError("Too few clusters found")
                    
            except Exception as e:
                logger.warning(f"BERTopic clustering failed or insufficient: {e}")
                # Стратегия 2: Fallback к адаптивному K-means
                logger.info("Falling back to adaptive K-means clustering...")
                topics, probabilities = await self.run_adaptive_kmeans_clustering(texts)
            
            clustering_time = time.time() - start_time
            logger.info(f"Text clustering completed in {clustering_time:.2f}s")
            
            return topics, probabilities
            
        except Exception as e:
            logger.error(f"Error during text clustering: {e}")
            raise
    
    async def cleanup(self):
        """Очистка ресурсов"""
        if self.executor:
            self.executor.shutdown(wait=True)

class ChatProcessor:
    """Main chat processing class with streaming summaries"""
    
    def __init__(self):
        self.bert_service = BERTClusteringService()
        self.text_generator = TextGenerator(VLLM_URL, SUMMARY_MODEL, END_TOKEN)
    
    async def initialize(self):
        """Initialize models"""
        logger.info("Initializing chat processor...")
        
        # Инициализация BERT сервиса
        await self.bert_service.initialize()
        
        logger.info("Chat processor initialized")
    
    def _extract_texts_from_messages(self, messages: List[Dict[str, Any]]) -> List[str]:
        """Extract text content from chat messages"""
        texts = []
        for msg_data in messages:
            try:
                message = ChatMessage.model_validate(msg_data)
                if message.text and message.text.strip():
                    texts.append(message.text.strip())
            except Exception as e:
                logger.warning(f"Failed to process message: {e}")
                continue
        
        logger.info(f"Extracted {len(texts)} valid texts from {len(messages)} messages")
        return texts
    
    async def cluster_chat_messages(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Cluster chat messages using BERT service"""
        logger.info(f"Clustering {len(messages)} chat messages...")
        start_time = time.time()
        
        try:
            # Извлечение текстов из сообщений
            texts = self._extract_texts_from_messages(messages)
            
            if not texts:
                raise ValueError("No valid text content found in messages")
            
            # Кластеризация через BERT сервис
            topics, probabilities = await self.bert_service.cluster_texts(texts)
            
            # Группировка сообщений по топикам (сохранение оригинальной структуры)
            topic_groups = {}
            for i, (msg_data, topic) in enumerate(zip(messages, topics)):
                if i >= len(texts):  # Пропуск сообщений без валидного текста
                    continue
                    
                topic_name = f"topic_{topic}" if topic >= 0 else "outliers"
                
                if topic_name not in topic_groups:
                    topic_groups[topic_name] = []
                
                # Добавление полных данных сообщения плюс информация о топике
                message_with_topic = msg_data.copy()
                message_with_topic.update({
                    "topic_id": topic,
                    "text_index": i
                })
                
                topic_groups[topic_name].append(message_with_topic)
            
            clustering_time = time.time() - start_time
            logger.info(f"Chat clustering completed in {clustering_time:.2f}s. Found {len(topic_groups)} topics")
            
            return {
                "topic_groups": topic_groups,
                "clustering_time": clustering_time,
                "total_messages": len(messages),
                "valid_texts": len(texts),
                "topics_found": len(topic_groups)
            }
        
        except Exception as e:
            logger.error(f"Error during chat clustering: {e}")
            raise
    
    async def save_all_topics_to_single_file(self, chat_name: str, request_id: str, topic_summaries: List[Dict[str, Any]]) -> str:
        """Save all chat topics to a single JSON file with simplified structure"""
        try:
            # Create simplified content structure without metadata
            topic_summaries.sort(key=lambda x: x["topic_id"])
            all_topics_content = {
                "topics": topic_summaries
            }

            # Generate safe filename
            safe_chat_name = "".join(c for c in chat_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"{safe_chat_name}_all_topics_{request_id}.json"
            filepath = os.path.join(TOPICS_DIR, filename)
            
            # Save asynchronously
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(all_topics_content, ensure_ascii=False, indent=2))
            
            logger.info(f"Saved all {len(topic_summaries)} topics to single file: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving all topics to single file: {e}")
            return ""
        
    async def summarize_chat_topic(self, messages: List[Dict[str, Any]], max_tokens: int = 16384) -> Tuple[str, str]:
        """Summarize chat topic using the text generator"""
        try:
            # Extract and combine texts from messages
            texts = []
            participants = set()
            
            for msg in messages:
                text = msg.get("text", "").strip()
                sender = msg.get("sender", "Unknown")
                if text:
                    texts.append(f"{sender}: {text}")
                    participants.add(sender)
            
            if not texts:
                return "error", "Нет содержания для суммирования"
            
            combined_text = "\n\n".join(texts)
            
            # Limit text length
            max_input_length = 8000
            if len(combined_text) > max_input_length:
                combined_text = combined_text[:max_input_length] + "..."
                logger.warning(f"Chat text truncated to {max_input_length} characters")
            
            # Create prompt for chat summarization with Gemma 3 12B IT tokens
            participants_list = ", ".join(list(participants))
            system_prompt = '''Вы система для суммаризации сообщения. Вы кратко пресказывается переданные вам сообщения и указываете тему обсуждения исходя из краткого пересказа.
            Формат вашего ответа это json -> {"summary": "Краткое описание темы", "topic_name": "Тема обсуждения"}'''
            prompt_1 = '''Кратко перескажи обсуждение в чате (максимум {} токенов) и укажи тему этого обсуждения.
            Участники: {}
            Сообщения чата:
            {}'''.format(max_tokens, participants_list, combined_text)
            prompt_2 = '{"summary":'
            
            # Use the text generator
            stop_sequences = [END_TOKEN, "Сообщения чата:", "Краткое описание темы:", "---"]
            answer = await self.text_generator.generate_text(
                system_prompt=system_prompt,
                prompt_1=prompt_1,
                prompt_2=prompt_2,
                max_tokens=max_tokens,
                temperature=0.15,
                top_p=0.8,
                stop_sequences=stop_sequences
            )
            
            temp = json.loads('{"summary":' + answer)
            return temp["topic_name"], temp["summary"]

        except Exception as e:
            logger.error(f"Error during chat summarization: {e}")
            return "error", f"Краткое описание недоступно: {str(e)}"
        
    async def generate_topic_summaries_stream(self, topic_groups: Dict[str, List]) -> AsyncGenerator[Dict[str, Any], List[Dict[str, Any]]]:
        """Generate topic summaries one by one using async generator"""
        total_topics = len(topic_groups)
        processed_count = 0
        
        logger.info(f"Starting streaming summary generation for {total_topics} topics")
        
        for topic_id, topic_messages in topic_groups.items():
            try:
                processed_count += 1
                logger.info(f"Processing topic {processed_count}/{total_topics}: {topic_id}")
                
                # Generate summary for this topic
                topic_name, summary = await self.summarize_chat_topic(topic_messages)
                
                # Get participants for this topic
                participants_topic = []
                for msg in topic_messages:
                    sender = msg.get("sender", "Unknown")
                    if sender not in participants_topic:
                        participants_topic.append(sender)
                
                # Yield topic summary result
                topic_summary = {
                    "topic_name": topic_name,
                    "summary": summary,
                    "message_count": len(topic_messages),
                    "topic_id": topic_messages[0].get("topic_id", -1) if topic_messages else -1,
                    "participants": participants_topic,
                    "progress": {
                        "current": processed_count,
                        "total": total_topics,
                    }
                }
                
                yield topic_summary, topic_messages
                
            except Exception as e:
                logger.error(f"Error processing topic {topic_id}: {e}")
                # Yield error result for this topic
                yield {
                    "topic_name": topic_id,
                    "summary": f"Ошибка генерации саммари: {str(e)}",
                    "message_count": len(topic_messages),
                    "topic_id": topic_messages[0].get("topic_id", -1) if topic_messages else -1,
                    "sample_messages": [],
                    "participants": [],
                    "error": str(e),
                    "progress": {
                        "current": processed_count,
                        "total": total_topics,
                        "percentage": round((processed_count / total_topics) * 100, 1)
                    }
                }, topic_messages
        
        logger.info(f"Completed streaming summary generation for {total_topics} topics")
    
    def _group_messages(self, messages: List[Dict[str, Any]], limit: int = 1) -> List[Dict[str, Any]]:
        """
        Группирует сообщения по следующим критериям:
        1. Объединяет сообщения от одного пользователя, если между ними меньше limit минут
        2. Объединяет сообщения от разных отправителей, если второе является коротким ответом (< 5 слов)
        
        Args:
            messages: Список сообщений в хронологическом порядке
            limit: Максимальная разница во времени в минутах для группировки (по умолчанию 2)
        
        Returns:
            Список сгруппированных сообщений
        """
        if not messages:
            return []
        
        if len(messages) == 1:
            return messages[:]
        
        result = []
        max_diff = timedelta(minutes=limit)
        
        # Начинаем с первого сообщения
        current_group = {
            "sender": messages[0]["sender"],
            "timestamp": messages[0]["timestamp"],
            "text": messages[0]["text"]
        }
        
        for i in range(1, len(messages)):
            prev_msg = messages[i-1]
            cur_msg = messages[i]
            
            # Парсим временные метки
            prev_time = datetime.fromisoformat(prev_msg["timestamp"])
            cur_time = datetime.fromisoformat(cur_msg["timestamp"])
            time_diff = abs(cur_time - prev_time)
            
            # Проверяем количество слов в текущем сообщении
            word_count = len(cur_msg["text"].split())
            
            # Условия для объединения:
            # 1. Тот же отправитель и разница во времени меньше лимита
            # 2. Разные отправители, но текущее сообщение короткое (< 5 слов)
            should_group = (
                (current_group["sender"] == cur_msg["sender"] and time_diff <= max_diff) or
                (current_group["sender"] != cur_msg["sender"] and word_count < 5)
            )
            
            if should_group:
                # Объединяем сообщения
                if current_group["sender"] == cur_msg["sender"]:
                    current_group["text"] += "\n{}".format(cur_msg["text"])
                else:
                    current_group["text"] += "\nПользователь {} ответил:\n{}".format(cur_msg["sender"], cur_msg["text"])
                # Обновляем временную метку на более позднюю
                if cur_time > datetime.fromisoformat(current_group["timestamp"]):
                    current_group["timestamp"] = cur_msg["timestamp"]
            else:
                # Добавляем текущую группу в результат и начинаем новую
                result.append(current_group)
                current_group = {
                    "sender": cur_msg["sender"],
                    "timestamp": cur_msg["timestamp"],
                    "text": cur_msg["text"]
                }
        
        # Добавляем последнюю группу
        result.append(current_group)
        
        return result

    async def process_chat_request_stream(self, request_data: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
        """Process chat request with streaming topic summaries"""
        request_id = request_data.get("request_id")
        chat_name = request_data.get("chat_name", "Unknown Chat")
        messages = request_data.get("messages", [])
        
        logger.info(f"Processing streaming chat request {request_id} for '{chat_name}' with {len(messages)} messages")
        
        try:
            # Validate input
            if not messages:
                raise ValueError("No messages provided in request")
            
            # Step 1: Send initial processing status
            yield {
                "request_id": request_id,
                "chat_name": chat_name,
                "status": "processing",
                "stage": "clustering",
                "timestamp": datetime.now().isoformat()
            }
            
            # Group messages
            messages = self._group_messages(messages)
            
            # Step 2: Cluster messages
            clustering_result = await self.cluster_chat_messages(messages)
            topic_groups = clustering_result["topic_groups"]
            logger.info("topic group: ", topic_groups)
            
            # Step 3: Send clustering completed with basic info
            yield {
                "request_id": request_id,
                "chat_name": chat_name,
                "status": "processing",
                "stage": "generating_summaries",
                "timestamp": datetime.now().isoformat()
            }
            
            # Step 4: Stream topic summaries one by one
            topic_summaries = []
            async for topic_summary, topic_messages in self.generate_topic_summaries_stream(topic_groups):
                
                # Send individual topic result
                yield {
                    "request_id": request_id,
                    "chat_name": chat_name,
                    "status": "processing",
                    "stage": "summary_generated",
                    "topic_summary": topic_summary,
                    "timestamp": datetime.now().isoformat()
                }

                topic_summary["messages"] = topic_messages
                topic_summaries.append(topic_summary)
            
            # Step 5: Save all topics to single file
            # timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            # single_file_path = await self.save_all_topics_to_single_file(
            #     chat_name, request_id, topic_summaries
            # )
            
            # Remove messages from topic_summaries for final response
            # for i, _ in enumerate(topic_summaries):
            #     del topic_summaries[i]["messages"]
            
            # Step 6: Send final completed result
            final_result = {
                "request_id": request_id,
                "chat_name": chat_name,
                "status": "completed",
                "stage": "finished",
                "topic_summaries": topic_summaries,
                "timestamp": datetime.now().isoformat()
            }
            
            yield final_result
            logger.info(f"Streaming chat request {request_id} for '{chat_name}' completed successfully")
        
        except Exception as e:
            logger.error(f"Error processing streaming chat request {request_id}: {e}")
            yield {
                "request_id": request_id,
                "chat_name": chat_name,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "stage": "error"
            }
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.text_generator.close()
        await self.bert_service.cleanup()

@dataclass
class TopicData:
    """Data structure for topic information"""
    request_id: str
    topic_id: str
    topic_name: str
    summary: str
    message_count: int
    topic_embedding: List[float]

@dataclass
class MessageData:
    """Data structure for message information"""
    request_id: str
    message_id: str
    topic_id: str
    sender: str
    timestamp: str
    message_text: str
    text_index: int
    message_embedding: List[float]

class RAGMemoryManager:
    """Manages conversation memory and semantic cache for RAG system"""
    
    def __init__(self, redis_url: str, vectorizer, ttl: int = 2 * 60 * 60):
        self.redis_url = redis_url
        self.vectorizer = vectorizer
        self.ttl = ttl
        self.session_manager = None
        self.semantic_cache = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize memory and cache components"""
        if self._initialized:
            return
            
        try:
            # Initialize session manager for conversation history
            self.session_manager = MessageHistory(
                name="rag_conversations",
                redis_url=self.redis_url,
                default_ttl=self.ttl
            )
            
            # Initialize semantic cache for answers
            self.semantic_cache = SemanticCache(
                name="rag_semantic_cache", 
                redis_url=self.redis_url,
                vectorizer=self.vectorizer,
                ttl=self.ttl,
                distance_threshold=0.25,
                filterable_fields=[{"name": "request_id", "type": "tag"}],
                # overwrite=True #при изменении схемы убрать первый комментарий
            )
            
            self._initialized = True
            logger.info("RAG memory and cache initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize RAG memory and cache: {e}")
            raise

    async def _ensure_initialized(self):
        """Ensure the manager is initialized before use"""
        if not self._initialized:
            await self.initialize()

    async def get_conversation_history(self, session_id: str, limit: int = 1) -> List[Dict[str, str]] | None:
        """Get conversation history for a request_id"""
        
        try:
            
            loop = asyncio.get_event_loop()
            messages = await loop.run_in_executor(
                None,
                lambda: self.session_manager.get_recent(
                    session_tag=session_id,
                    top_k=limit
                )
            )
            if len(messages) < 1:
                return None
            
            # Convert to our format
            history = []
            for msg in messages:
                if isinstance(msg, dict):
                    history.append({
                        "role": msg.get("role", "user"),
                        "content": msg.get("content", ""),
                    })
            if len(history) < 1:
                return None
            return history
            
        except Exception as e:
            logger.error(f"Failed to get conversation history for {session_id}: {e}")
            return []

    async def add_to_conversation_history(self, session_id: str, query: str, answer: str, 
                                        query_timestamp: str = None, answer_timestamp: str = None):
        """Add query-answer pair to conversation history"""
        
        try:
            if query_timestamp is None: 
                query_timestamp = datetime.now().isoformat()
            if answer_timestamp is None:
                answer_timestamp = datetime.now().isoformat()
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.session_manager.add_message(
                    message={
                        "role": "user",
                        "content": query,
                        "timestamp": query_timestamp
                    },
                    session_tag=session_id
                )
            )
            
            await loop.run_in_executor(
                None,
                lambda: self.session_manager.add_message(
                    message={
                        "role": "llm", 
                        "content": answer,
                        "timestamp": answer_timestamp
                    },
                    session_tag=session_id
                )
            )
        
            logger.info(f"Added conversation pair to history for {session_id}")
            
        except Exception as e:
            logger.error(f"Failed to add to conversation history: {e}")
            raise

    async def check_semantic_cache(self, query: str, request_id: str) -> Optional[str]:
        """Check if answer exists in semantic cache"""
        
        try:
            logger.info(f"Checking cache for query in {request_id}: {query[:50]}...")
            
            loop = asyncio.get_event_loop()
            cached_response = await loop.run_in_executor(
                None,
                lambda: self.semantic_cache.check(
                    prompt=query,
                    filter_expression=(Tag("request_id") == request_id),
                    return_fields=["prompt", "response"]
                )
            )
            # logger.info(cached_response)
            if cached_response:
                logger.info(f"Cache hit for query in {request_id}: {cached_response[0]}...")
                return cached_response[0].get("response")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to check semantic cache: {e}")
            return None

    async def store_in_semantic_cache(self, query: str, request_id: str, answer: str):
        """Store answer in semantic cache"""
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.semantic_cache.store(
                    prompt=query,
                    response=answer,
                    metadata={
                        "timestamp": datetime.now().isoformat()
                    },
                    filters={
                        "request_id": request_id
                    }
                )
            )
            
            logger.info(f"Stored answer in cache for {request_id}: {query[:50]}...")
            
        except Exception as e:
            logger.error(f"Failed to store in semantic cache: {e}")
            raise

    async def get_contextualized_history(self, request_id: str, current_query: str, 
                                       limit: int = 5) -> str:
        """Get formatted conversation history for context"""
        
        try:
            history = await self.get_conversation_history(request_id, limit * 2)  # Get more to filter pairs
            logger.info(history)
            if history is None:
                return current_query
            
            # Group messages into pairs
            context_lines = []
            for i in range(0, len(history) - 1, 2):
                if (i + 1 < len(history) and 
                    history[i]["role"] == "user" and 
                    history[i + 1]["role"] in ["assistant", "llm"]):
                    
                    user_msg = history[i]["content"]
                    assistant_msg = history[i + 1]["content"]
                    context_lines.append(f"Пользователь: {user_msg}")
                    context_lines.append(f"Ассистент: {assistant_msg}")
                    
                    if len(context_lines) >= limit * 2:  # limit pairs
                        break
            
            if context_lines:
                return "История диалога:\n" + "\n".join(context_lines) + "\n\nТекущий вопрос: " + current_query
            
            return current_query
            
        except Exception as e:
            logger.error(f"Failed to get contextualized history: {e}")
            return current_query

    async def clear_conversation_history(self, request_id: str):
        """Clear conversation history for a request_id"""
        
        try:
            session_id = request_id
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.session_manager.delete_session(session_id)
            )
            
            logger.info(f"Cleared conversation history for {request_id}")
            
        except Exception as e:
            logger.error(f"Failed to clear conversation history: {e}")
            raise

    async def cleanup(self):
        """Clean up memory and cache resources"""
        try:
            # tasks = []
            
            # if self.session_manager and hasattr(self.session_manager, 'aclose'):
            #     tasks.append(self.session_manager.aclose())
            # elif self.session_manager and hasattr(self.session_manager, 'close'):
            #     loop = asyncio.get_event_loop()
            #     tasks.append(loop.run_in_executor(None, self.session_manager.close))
                
            # if self.semantic_cache and hasattr(self.semantic_cache, 'aclose'):
            #     tasks.append(self.semantic_cache.aclose())
            # elif self.semantic_cache and hasattr(self.semantic_cache, 'close'):
            #     loop = asyncio.get_event_loop()
            #     tasks.append(loop.run_in_executor(None, self.semantic_cache.close))
            
            # if tasks:
            #     await asyncio.gather(*tasks, return_exceptions=True)
                
            self._initialized = False
            logger.info("RAG memory and cache cleanup completed")
                
        except Exception as e:
            logger.error(f"Error during memory/cache cleanup: {e}")  

class RedisVLIndexManager:
    """Manager for RedisVL indices creation and management"""
    
    def __init__(self, bert_service, redis_url: str = REDIS_URL, ttl: int = 2 * 60 * 60):
        self.redis_url = redis_url
        self.topics_index = None
        self.messages_index = None
        self.bert_service = bert_service
        self.vectorizer = None
        self.ttl = ttl
        self.memory_manager = None
      
    async def initialize_indices(self, topics_schema_path: str, messages_schema_path: str):
        """Initialize both topic and message indices - UPDATED"""
        try:
            # Load schemas
            topics_schema = IndexSchema.from_yaml(topics_schema_path)
            messages_schema = IndexSchema.from_yaml(messages_schema_path)
            
            # Create indices
            self.topics_index = AsyncSearchIndex(
                schema=topics_schema,
                redis_url=self.redis_url
            )
            
            self.messages_index = AsyncSearchIndex(
                schema=messages_schema,
                redis_url=self.redis_url
            )
            
            # Create indices if they don't exist
            await self.topics_index.create(overwrite=False)
            await self.messages_index.create(overwrite=False)
            
            # Initialize vectorizer
            self.vectorizer = CustomTextVectorizer(embed=self.generate_embedding, aembed=self.agenerate_embedding)
            
            # NEW: Initialize memory manager
            self.memory_manager = RAGMemoryManager(
                redis_url=self.redis_url,
                vectorizer=self.vectorizer,
                ttl=self.ttl
            )
            await self.memory_manager.initialize()
            
            logger.info("RedisVL indices and memory manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize RedisVL indices: {e}")
            raise

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using BERT service"""
        try:
            if not self.bert_service.embedding_model:
                raise ValueError("BERT service not initialized")
            
            # Use the existing embedding model from BERTClusteringService
            embedding = self.bert_service.embedding_model.encode(text)
            
            return embedding.tolist()
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    async def agenerate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using BERT service"""
        try:
            if not self.bert_service.embedding_model:
                raise ValueError("BERT service not initialized")
            
            # Use the existing embedding model from BERTClusteringService
            loop = asyncio.get_event_loop()
            embedding = await loop.run_in_executor(
                self.bert_service.executor,
                self.bert_service.embedding_model.encode,
                text
            )
            
            return embedding.tolist()
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
    
    async def load_topics_data(self, topics_data: List[TopicData]):
        """Load topic data into Redis index"""
        try:
            data_to_load = []
            for topic in topics_data:
                topic_doc = {
                    "request_id": topic.request_id,
                    "topic_id": str(topic.topic_id),
                    "topic_name": topic.topic_name,
                    "summary": topic.summary,
                    "message_count": topic.message_count,
                    "topic_embedding": array_to_buffer(topic.topic_embedding, dtype="float32")
                }
                data_to_load.append(topic_doc)
            
            await self.topics_index.load(data_to_load, ttl=self.ttl)
            logger.info(f"Loaded {len(data_to_load)} topics into Redis index")
            
        except Exception as e:
            logger.error(f"Failed to load topics data: {e}")
            raise
        
    def timestamp_convert(self, timestamp_str):
        """Конвертирует ISO 8601 timestamp в Unix timestamp для Redis"""
        try:
            # datetime.now().isoformat() возвращает формат: '2024-01-01T15:01:00.123456'
            # Парсим этот формат напрямую
            dt = datetime.fromisoformat(timestamp_str)
            # Конвертируем в Unix timestamp (целое число секунд)
            return int(dt.timestamp())
        except Exception as e:
            print(f"Error converting timestamp {timestamp_str}: {e}")
            return int(time.time())  # Возвращаем текущее время как fallback

    async def load_messages_data(self, messages_data: List[MessageData]):
        """Load message data into Redis index"""
        try:
            data_to_load = []
            for message in messages_data:
                message_doc = {
                    "request_id": message.request_id,
                    "message_id": message.message_id,
                    "topic_id": str(message.topic_id),
                    "sender": message.sender,
                    "timestamp": self.timestamp_convert(message.timestamp),
                    "message_text": message.message_text,
                    "text_index": message.text_index,
                    "message_embedding": array_to_buffer(message.message_embedding, dtype="float32")
                }
                data_to_load.append(message_doc)
            # print(message_doc[0])
            await self.messages_index.load(data_to_load, ttl=self.ttl)
            logger.info(f"Loaded {len(data_to_load)} messages into Redis index")
            
        except Exception as e:
            logger.error(f"Failed to load messages data: {e}")
            raise
    
    async def check_topics(self, text_request_id: str) -> bool:
        """Check is available topics for request_id"""
        try:
            filter_expression = (Tag("request_id") == text_request_id)
            query = CountQuery(
                filter_expression=filter_expression
            )
            ans = await self.topics_index.query(query)
            # logger.info(f"Topics count: {ans}")
            if ans > 0:
                return True
            else:
                return False
            
            
        except Exception as e:
            logger.error(f"Failed to search topics: {e}")
            raise

    async def get_topics(self, text_request_id: str) -> List[str]:
        """Get topics for request_id"""
        filter_expression = Tag("request_id") == text_request_id
        lim_query = CountQuery(
                filter_expression=filter_expression
        )
        limit = await self.topics_index.query(lim_query)
        query = FilterQuery(
            return_fields=["summary"],
            filter_expression=filter_expression,
            num_results=limit
        )
        ans = await self.topics_index.query(query)
        return [topic.get("summary", "") for topic in ans]

    async def search_topics(self, query_embedding: List[float], text_request_id: str, top_k: int = 3) -> Tuple[List[str], List[str]]:
        """Search for relevant topics using vector similarity"""
        try:
            filter_expression = (Tag("request_id") == text_request_id)
            print(text_request_id)
            # query = VectorQuery(
            #     vector=query_embedding,
            #     vector_field_name="topic_embedding",
            #     filter_expression=filter_expression,
            #     return_fields=["topic_id", "topic_name", "summary"],
            #     num_results=top_k
            # )
            query = VectorQuery(
                vector=query_embedding,
                vector_field_name="topic_embedding",
                return_fields=["topic_id", "topic_name", "summary", "request_id"],
                num_results=top_k,
                return_score=False
            )
            query.set_filter(filter_expression)
            # print(query.query)
            # print(query.params)
            ans = await self.topics_index.query(query)
            # logger.info(ans)
            result = []
            summar = []
            for topic in ans:
                id = topic["topic_id"]
                # logger.info(topic)
                # print(topic["request_id"]==text_request_id)
                if id not in result:
                    result.append(id)
                    summar.append((topic["summary"], id))
            return result, summar
            
            
        except Exception as e:
            logger.error(f"Failed to search topics: {e}")
            raise
    
    async def search_messages_in_topic(self, topic_ids: List[str], query_embedding: List[float], 
                                     query_text: str, text_request_id: str,top_m: int = 10, alpha: float = 0.5) -> List[Tuple[str]]:
        """Search for relevant messages in a specific topic using hybrid search"""
        try:
            filter_expression = ((Tag("request_id") == text_request_id) &(Tag("topic_id") == topic_ids))
            hybrid_query = HybridQuery(
                vector=query_embedding,
                vector_field_name="message_embedding",
                text=query_text,
                text_field_name="message_text",
                return_fields=["sender", "timestamp", "message_text", "topic_id"],
                filter_expression=filter_expression,
                num_results=top_m,
                stopwords=None,
                alpha=alpha  # Weight between vector (0) and text (1) search
                # return_score=False
            )


            logger.info("Searching messages in topic: {}".format(",".join(topic_ids)))
            results = await self.messages_index.query(hybrid_query)
            # for res in results:
            #     print(res.get("topic_id") in topic_ids)
            return [("{} написал(а) в {}: {}".format(r["sender"], (datetime.fromtimestamp(int(r["timestamp"]))).strftime('%d.%m.%Y %H:%M:%S'), r["message_text"]), r["topic_id"]) for r in results]
            # return results
            
        except Exception as e:
            logger.error("Failed to search messages in topic {}: {}".format(",".join(topic_ids), e))
            raise
    
    async def cleanup(self):
        """Clean up resources - UPDATED"""
        try:
            if self.topics_index:
                await self.topics_index.disconnect()
            if self.messages_index:
                await self.messages_index.disconnect()
            # NEW: Cleanup memory manager
            if self.memory_manager:
                await self.memory_manager.cleanup()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

class RAGChatSystem:
    """Main RAG system for chat processing"""
    
    def __init__(self, bert_service, redis_url: str = REDIS_URL, 
                 topics_schema_path: str = "topics_schema.yaml", messages_schema_path: str = "messages_schema.yaml"):
        self.index_manager = RedisVLIndexManager(bert_service, redis_url)
        self.topics_schema_path = topics_schema_path
        self.messages_schema_path = messages_schema_path
        self.initialized = False
    
    async def initialize(self):
        """Initialize the RAG system"""
        try:
            await self.index_manager.initialize_indices(
                self.topics_schema_path, 
                self.messages_schema_path
            )
            self.initialized = True
            logger.info("RAG system initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize RAG system: {e}")
            raise
    
    async def index_chat_data(self, chat_data: Dict[str, Any], request_id: str):
        """Index chat data (topics and messages) into Redis"""
        try:
            if not self.initialized:
                await self.initialize()
            
            topics_data = []
            messages_data = []
            
            for topic_info in chat_data.get("topics", []):
                topic_name = topic_info["topic_name"]
                summary = topic_info["summary"]
                topic_id = topic_info["topic_id"]
                message_count = topic_info["message_count"]
                
                # Generate topic embedding
                topic_text = f"{topic_name} {summary}"
                topic_embedding = await self.index_manager.vectorizer.aembed(topic_text)
                
                topics_data.append(TopicData(
                    request_id=request_id,
                    topic_id=str(topic_id),
                    topic_name=topic_name,
                    summary=summary,
                    message_count=message_count,
                    topic_embedding=topic_embedding
                ))
                # logger.info(len(topic_info.get("messages", [])))
                # Process messages for this topic
                for message in topic_info.get("messages", []):
                    message_id = f"{topic_id}_{message['text_index']}"
                    mes_text = "{} сказал(а): {}".format(message["sender"], message["text"])
                    # Generate message embedding
                    message_embedding = await self.index_manager.vectorizer.aembed(mes_text)
                    
                    messages_data.append(MessageData(
                        request_id=request_id,
                        message_id=message_id,
                        topic_id=str(topic_id),
                        sender=message["sender"],
                        timestamp=message["timestamp"],
                        message_text=message["text"],
                        text_index=message["text_index"],
                        message_embedding=message_embedding
                    ))
            # logger.info(messages_data)
            # Load data into Redis indices
            # for mes in messages_data:
            #     print(mes.message_text)
            await self.index_manager.load_topics_data(topics_data)
            await self.index_manager.load_messages_data(messages_data)
            
            logger.info(f"Indexed {len(topics_data)} topics and {len(messages_data)} messages")
            
        except Exception as e:
            logger.error(f"Failed to index chat data: {e}")
            raise
    
    async def search_chat(self, query: str, text_request_id: str, top_k_topics: int = 3, 
                         top_m_messages: int = 5, alpha: float = 0.5) -> Tuple[List[str], List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Perform two-stage RAG search"""
        try:
            if not self.initialized:
                await self.initialize()
            
            # Generate query embedding
            query_embedding = await self.index_manager.vectorizer.aembed(query)
            
            # Stage 1: Find relevant topics
            relevant_topics, summary_of_topics = await self.index_manager.search_topics(
                query_embedding, text_request_id, top_k_topics
            )
            # logger.info(relevant_topics)
            # Stage 2: Search messages in each relevant topic
            search_results = await self.index_manager.search_messages_in_topic(
                topic_ids=relevant_topics,
                query_embedding=query_embedding,
                query_text=query,
                text_request_id=text_request_id,
                top_m=top_m_messages,
                alpha=alpha
            )
            # logger.info(search_results)
            summary_of_topics.sort(key=lambda x: x[1])
            search_results.sort(key=lambda x: x[1])
            result = [x[0] for x in search_results]
                
            
            # logger.info(f"RAG search completed: found {len(relevant_topics)} topics")
            return result, summary_of_topics, search_results
            
        except Exception as e:
            logger.error(f"Failed to perform RAG search: {e}")
            raise
    
    async def cleanup(self):
        """Clean up resources"""
        await self.index_manager.cleanup()

class RAGProcessor:
    """Processor that integrates RAG with existing chat processing"""
    
    def __init__(self, chat_processor, redis_url: str = REDIS_URL):
        self.chat_processor: ChatProcessor = chat_processor
        self.rag_system = RAGChatSystem(
            bert_service=chat_processor.bert_service,
            redis_url=redis_url
        )
    
    async def initialize(self):
        """Initialize RAG processor"""
        await self.rag_system.initialize()
    
    async def process_and_index_chat(self, request_data: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
        """Process chat using existing pipeline and index results for RAG"""
        try:
            # Process chat using existing functionality
            final_result = None
            async for result in self.chat_processor.process_chat_request_stream(request_data):
                final_result = result
                if result.get("status") == "completed":
                    break
                else:
                    yield result
            
            if not final_result or final_result.get("status") != "completed":
                raise ValueError("Chat processing failed")
            
            # Convert to format expected by RAG system
            rag_data = self._convert_to_rag_format(final_result)
            
            # Index the processed data
            await self.rag_system.index_chat_data(rag_data, request_data["request_id"])
            
            # Add RAG capabilities to the result
            final_result["rag_enabled"] = True
            final_result["indexed_topics"] = len(rag_data["topics"])
            final_result["indexed_messages"] = sum(len(topic["messages"]) for topic in rag_data["topics"])
            for mes in final_result["topic_summaries"]:
                del mes["messages"]
            yield final_result
            
        except Exception as e:
            logger.error(f"Failed to process and index chat: {e}")
            raise
    
    def _convert_to_rag_format(self, processed_result: Dict[str, Any]) -> Dict[str, Any]:
        """Convert processed chat result to RAG format"""
        try:
            rag_format = {"topics": []}
            
            for topic_summary in processed_result.get("topic_summaries", []):
                topic_data = {
                    "topic_id": topic_summary["topic_id"],
                    "topic_name": topic_summary["topic_name"],
                    "summary": topic_summary["summary"],
                    "message_count": topic_summary["message_count"],
                    "messages": topic_summary.get("messages", [])
                }
                rag_format["topics"].append(topic_data)
            
            return rag_format
            
        except Exception as e:
            logger.error(f"Failed to convert to RAG format: {e}")
            raise
    
    async def check_safe_questions(self, query: str) -> bool:
        stop_sequences = [END_TOKEN, "---"]
        delimiter = "\n\n---\n\n"
        instruction = '''
        Вы - система валидации вопросов.
        Ваша задача - определять является ли запрос пользователя командой или вопросом по сообщениям.

        Перед тем как дать финальный ответ, внимательно проанализируйте пошагово. Обратите особое внимание на формулировку вопроса.
        - Помните, что команда можеть быть замаскирована под вопрос, но она всё равно требует каких-то конкретных действий не связанных с поиском информации.
        - Помните, что в команде обычно содержится какой-то приказ или просьба для тебя.
        - Помните, что если запрос не содержит указания конкретных действий, то, скоррее всего, это вопрос.
        - Помните, что если запрос требует информацию об участнике чата, то, скоррее всего, это вопрос.
        '''
        user_prompt = '''
        Вот запрос пользователя:
        {}
        '''.format(query)
        pydantic_schema = re.sub(r"^ {4}", "", inspect.getsource(Safe_Query_Schema), flags=re.MULTILINE)
        # logger.info(pydantic_schema)
        schema = f"Ваш ответ должен быть в формате JSON и строго следовать данной схеме, заполняя поля в указанном порядке:\n```\n{pydantic_schema}\n```"
        sys_safe_prompt =  instruction.strip() + schema.strip() + delimiter
        user_safe_prompt = user_prompt.strip() + delimiter
        answer = await self.chat_processor.text_generator.generate_text(system_prompt=sys_safe_prompt, 
                                                                        prompt_1=user_safe_prompt, 
                                                                        max_tokens=8192, 
                                                                        stop_sequences=stop_sequences, 
                                                                        temperature=0.05)
        r = answer.find("{")
        l = answer.rfind("}")
        if r == -1:
            answer = "{" + answer
            r = 0
        if l == -1:
            answer = answer + "}"
        logger.info(f"Answer: {answer}")
        ans = json.loads(answer[r:l+1])["final_answer"]
        logger.info(f"Safe: {ans}")
        if ans == "query":
            return True
        else:
            return False
        
    async def check_enough_info(self, messages: list, query: str) -> Tuple[str, Literal["enough", "extra"]]:
        stop_sequences = [END_TOKEN, "---"]
        delimiter = "\n\n---\n\n"
        instruction = '''
        Вы - система для определения возможности ответа на вопрос. Вы также часть системы ответов на вопросы с использованием RAG (Retrieval-Augmented Generation).
        Ваша задача - определять возможно ли ответить на вопрос опираясь только на предоставленные сообщения, которые были извлечены с помощью RAG. 
        Также ваша задача либо создавать дополнительный вопрос при недостатке информации, либо давать ответ на вопрос при наличии достаточного количества информации.

        Перед тем как дать ответ, внимательно проанализируйте пошагово. Обратите особое внимание на формулировку вопроса.
        - Помните, что необходимо опираться только на предоставленные знания.
        - Помните, что нельзя использовать собственные знания.
        - Учтите, что ответ на вопрос может находиться в нескольких сообщениях по частям.
        - Помните, что контент с ответом может быть сформулирован иначе, чем вопрос.
        - Вопрос может быть автоматически сгенерирован, поэтому он может быть неуместным или неприменимым к данным сообщениям.
        '''
        user_prompt = '''
        Вот контекст:
        \"\"\"
        {}
        \"\"\"

        ---
        Вот запрос пользователя:
        {}
        '''.format("'\n'---\n".join(messages), query)
        pydantic_schema = re.sub(r"^ {4}", "", inspect.getsource(Enough_Query_Schema), flags=re.MULTILINE)
        # logger.info(pydantic_schema)
        schema = f"Ваш ответ должен быть в формате JSON и строго следовать данной схеме, заполняя поля в указанном порядке:\n```\n{pydantic_schema}\n```"
        sys_safe_prompt =  instruction.strip() + schema.strip() + delimiter
        user_safe_prompt = user_prompt.strip() + delimiter
        answer = await self.chat_processor.text_generator.generate_text(system_prompt=sys_safe_prompt, 
                                                                        prompt_1=user_safe_prompt, 
                                                                        max_tokens=16384, 
                                                                        stop_sequences=stop_sequences)
        r = answer.find("{")
        l = answer.rfind("}")
        if r == -1:
            answer = "{" + answer
            r = 0
        if l == -1:
            answer = answer + "}"
        ans_model = Enough_Query_Schema.model_validate_json(answer[r:l+1])
        status = ans_model.status
        ans = ans_model.final_answer
        logger.info(f"Status: {status}, Answer: {ans}")
        return ans, status
    
    async def gen_answer(self, messages: list, query: str) -> Tuple[str, Literal["N/A"]]:
        stop_sequences = [END_TOKEN, "---"]
        delimiter = "\n\n---\n\n"
        instruction = '''
        Вы - система генерации ответов на вопросы с использованием RAG (Retrieval-Augmented Generation).
        Ваша задача - отвечать на вопрос опираясь только на предоставленные сообщения, которые были извлечены с помощью RAG. 

        Перед тем как дать ответ, внимательно проанализируйте пошагово. Обратите особое внимание на формулировку вопроса.
        - Помните, что необходимо опираться только на предоставленный контекст.
        - Помните, что нельзя использовать собственные знания.
        - Учтите, что ответ на вопрос может находиться в нескольких сообщениях по частям.
        - Помните, что контент с ответом может быть сформулирован иначе, чем вопрос.
        - Вопрос может быть автоматически сгенерирован, поэтому он может быть неуместным или неприменимым к данным сообщениям.
        - Помните, что в контексте может отсутствовать ответ на вопрос 
        '''
        user_prompt = '''
        Вот контекст:
        \"\"\"
        {}
        \"\"\"

        ---
        Вот запрос пользователя:
        {}
        '''.format("'\n---\n'".join(messages), query)
        pydantic_schema = re.sub(r"^ {4}", "", inspect.getsource(Answer_Schema), flags=re.MULTILINE)
        # logger.info(pydantic_schema)
        schema = f"Ваш ответ должен быть в формате JSON и строго следовать данной схеме, заполняя поля в указанном порядке:\n```\n{pydantic_schema}\n```"
        sys_safe_prompt =  instruction.strip() + schema.strip() + delimiter
        user_safe_prompt = user_prompt.strip() + delimiter
        answer = await self.chat_processor.text_generator.generate_text(system_prompt=sys_safe_prompt, 
                                                                        prompt_1=user_safe_prompt, 
                                                                        max_tokens=16384, 
                                                                        stop_sequences=stop_sequences)
        r = answer.find("{")
        l = answer.rfind("}")
        if r == -1:
            answer = "{" + answer
            r = 0
        if l == -1:
            answer = answer + "}"
        try:
            ans_model = Answer_Schema.model_validate_json(answer[r:l+1])
            ans = ans_model.final_answer
            logger.info(f"Answer: {ans}")
            return ans
        except Exception as e:
            logger.error(f"Answer parsing error: {e}")
            return "N/A"

    async def requery(self, query: str, extra_context) -> List[str]:
        stop_sequences = [END_TOKEN, "---"]
        delimiter = "\n\n---\n\n"
        instruction = '''
        Вы - система для перфразирования вопросов. Вы также часть системы ответов на вопросы с использованием RAG (Retrieval-Augmented Generation).
        Ваша задача - перфразировать вопрос для оптимизации поиска информации с помощью RAG по сообщениям из чата. 
        Также ваша задача разделять составные вопросы на их простые части. 
        Для более точной работы используй предоставленные в чате темы.

        Перед тем как дать ответ, внимательно проанализируйте пошагово. Обратите особое внимание на формулировку вопроса.
        - Помните, что необходимо сохранить суть вопроса.
        - Учтите, что в составном вопросе, могут быть опущены некоторые слова.
        - Помните, что в вопросе могут быть различные абревиатуры и слэнговые выражения.
        - Опирайтесь на представленные темы в чате
        '''
        user_prompt = '''
        Представленные темы:
        {}

        Вот запрос пользователя:
        {}
        '''.format("\n---\n".join(extra_context), query)
        example = Requering_schema.example
        pydantic_schema = re.sub(r"^ {4}", "", inspect.getsource(Requering_schema.Requery_Schema), flags=re.MULTILINE)
        # logger.info(pydantic_schema)
        schema = f"Ваш ответ должен быть в формате JSON и строго следовать данной схеме, заполняя поля в указанном порядке:\n```\n{pydantic_schema}\n```"
        sys_safe_prompt =  instruction.strip() + schema.strip() + example.strip() + delimiter
        user_safe_prompt = user_prompt.strip() + delimiter
        answer = await self.chat_processor.text_generator.generate_text(system_prompt=sys_safe_prompt, 
                                                                        prompt_1=user_safe_prompt, 
                                                                        max_tokens=16384, 
                                                                        stop_sequences=stop_sequences,
                                                                        temperature=0.1)
        r = answer.find("{")
        l = answer.rfind("}")
        if r == -1:
            answer = "{" + answer
            r = 0
        if l == -1:
            answer = answer + "}"
        try:
            ans_model = Requering_schema.Requery_Schema.model_validate_json(answer[r:l+1])
            ans = ans_model.final_answer
            logger.info(f" Answer: {ans}")
            return ans
        except Exception as e:
            logger.error(f"Answer parsing error: {e}")
            return [query]
    
    async def gen_answer_deep_logic(self, topics: list, messages: list, query: str) -> Tuple[str, Literal["N/A"]]:
        context = {}
        ind = 0
        for summa, id in topics:
            context[id] = {"Пересказ": summa, "Сообщения": []}
            while ind < len(messages) and messages[ind][0] == id:
                context[id]["Сообщения"].append(messages[ind][0])
                ind += 1
        stop_sequences = [END_TOKEN, "---"]
        delimiter = "\n\n---\n\n"
        instruction = '''
        Вы - система генерации ответов на вопросы с использованием RAG (Retrieval-Augmented Generation) с использованием глубокой логики.
        Ваша задача -  отвечать на вопрос гипотезой, опираясь только на предоставленные сообщения, которые были извлечены с помощью RAG, и на ваши размышления, основанные на предоставленном контексте.

        Перед тем как дать ответ, внимательно проанализируйте пошагово. Обратите особое внимание на формулировку вопроса.
        - Помните, что необходимо опираться на предоставленный контекст.
        - Учтите, что ответ на вопрос может находиться в нескольких сообщениях по частям.
        - Помните, что контекст с ответом может быть сформулирован иначе, чем вопрос.
        - Помните, что в контексте может отсутствовать ответ на вопрос.
        '''
        user_prompt = '''
        Вот контекст:
        \"\"\"
        {}
        \"\"\"

        ---
        Вот запрос пользователя:
        {}
        '''.format(json.dumps(context), query)
        pydantic_schema = re.sub(r"^ {4}", "", inspect.getsource(Deep_Logic_Schema), flags=re.MULTILINE)
        # logger.info(pydantic_schema)
        schema = f"Ваш ответ должен быть в формате JSON и строго следовать данной схеме, заполняя поля в указанном порядке:\n```\n{pydantic_schema}\n```"
        sys_safe_prompt =  instruction.strip() + schema.strip() + delimiter
        user_safe_prompt = user_prompt.strip() + delimiter
        answer = await self.chat_processor.text_generator.generate_text(system_prompt=sys_safe_prompt, 
                                                                        prompt_1=user_safe_prompt, 
                                                                        max_tokens=16384, 
                                                                        stop_sequences=stop_sequences,
                                                                        temperature=0.5)
        r = answer.find("{")
        l = answer.rfind("}")
        if r == -1:
            answer = "{" + answer
            r = 0
        if l == -1:
            answer = answer + "}"
        logger.info(f"Answer: {answer}")
        try:
            ans_model = Deep_Logic_Schema.model_validate_json(answer[r:l+1])
            ans = ans_model.final_answer
            logger.info(f"Answer: {ans}")
            return "Предположение: " + ans
        except Exception as e:
            logger.error(f"Answer parsing error: {e}")
            return "N/A"

    async def search(self, query: str, text_request_id: str, use_history: bool = True, 
                    use_cache: bool = True, **kwargs) -> Tuple[Literal[0, 1, 2], str]:
        """Search using RAG system with memory and cache - UPDATED"""
        try:
            if not await self.check_safe_questions(query):
                return 0, "Не является вопросом"
            
            # NEW: Check semantic cache first if enabled
            if use_cache and self.rag_system.index_manager.memory_manager:
                cached_answer = await self.rag_system.index_manager.memory_manager.check_semantic_cache(
                    query, text_request_id
                )
                if cached_answer:
                    logger.info(f"Returning cached answer for query: {query[:50]}...")
                    return 1, cached_answer
            
            # NEW: Get conversation history for context if enabled
            contextualized_query = query
            if use_history and self.rag_system.index_manager.memory_manager:
                contextualized_query = await self.rag_system.index_manager.memory_manager.get_contextualized_history(
                    text_request_id, query, limit=1
                )
                if contextualized_query != query:
                    logger.info(f"Using contextualized query with history")
            
            # Perform requery with potentially contextualized input
            topics = await self.rag_system.index_manager.get_topics(text_request_id)
            quest = await self.requery(contextualized_query, topics)
            context = []
            summa_context = []
            messa_context = []
            
            for que in quest:
                res, summa_id, messa_id = await self.rag_system.search_chat(que, text_request_id, **kwargs)
                summa_context.extend(summa_id)
                messa_context.extend(messa_id)
                ans = await self.gen_answer(res, query)
                if ans == "N/A":
                    continue
                context.append(f"Вопрос: {que}\nОтвет: {ans}")
                logger.info(context[-1])
            
            final_answer = "Не удалось получить ответ"
            status = 2 
            
            if len(context) > 0:
                final_answer = await self.gen_answer(context, query)
                if final_answer != "N/A":
                    status = 1
            
            if status == 2 and len(summa_context) > 0:
                final_answer = await self.gen_answer_deep_logic(summa_context, messa_context, query)
                if final_answer != "N/A":
                    status = 1
            
            # NEW: Store successful answers in cache and conversation history
            if status == 1 and self.rag_system.index_manager.memory_manager:
                if use_cache:
                    await self.rag_system.index_manager.memory_manager.store_in_semantic_cache(
                        query, text_request_id, final_answer
                    )
                
                # Always store in conversation history for successful queries
                await self.rag_system.index_manager.memory_manager.add_to_conversation_history(
                    text_request_id, query, final_answer
                )
            
            return status, final_answer
            
        except Exception as e:
            logger.error(f"Error in RAG search with memory: {e}")
            return 2, "Ошибка при поиске"


    
    async def cleanup(self):
        """Clean up resources"""
        await self.rag_system.cleanup()

class KafkaProcessor:
    """Kafka consumer and producer"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.chat_processor = ChatProcessor()
        self.rag_processor = None
        self.running = False
    
    async def start(self):
        """Start Kafka connections and processors"""
        logger.info("Starting Kafka processor support...")
        
        # Initialize chat processor
        await self.chat_processor.initialize()
        
        # Initialize RAG processor
        self.rag_processor = RAGProcessor(self.chat_processor)
        await self.rag_processor.initialize()
        
        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        # Initialize Kafka producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            retry_backoff_ms=1000,
            max_batch_size=16384
        )
        
        await self.consumer.start()
        await self.producer.start()
        
        logger.info("Kafka processor started")
        self.running = True
    
    async def process_messages(self):
        """Main message processing loop"""
        logger.info("Starting message processing loop")
        
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    request_data = message.value
                    request_type = request_data.get("type", "process_chat")
                    
                    if request_type == "process_chat":
                        await self._handle_chat_processing(request_data)
                    elif request_type == "rag_search":
                        await self._handle_rag_search(request_data)
                    elif request_type == "clear_history": 
                        await self._handle_clear_history(request_data)
                    else:
                        logger.warning(f"Unknown request type: {request_type}")
                        await self._send_error_response(
                            request_data, 
                            f"Unknown request type: {request_type}"
                        )
                
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await self._send_error_response(
                        request_data if 'request_data' in locals() else {}, 
                        str(e)
                    )
        
        except Exception as e:
            logger.error(f"Error in message processing loop: {e}")
    
    async def _handle_chat_processing(self, request_data: Dict[str, Any]):
        """Handle chat processing with RAG indexing"""
        request_id = request_data.get("request_id")
        chat_name = request_data.get("chat_name", "Unknown Chat")
        enable_rag = request_data.get("enable_rag", True)
        
        logger.info(f"Processing chat request {request_id} for '{chat_name}' (RAG: {enable_rag})")
        
        try:
            if enable_rag:
                # Process with RAG support
                await self._process_chat_with_rag(request_data)
            else:
                # Process with original streaming method
                await self._process_chat_streaming(request_data)
                
        except Exception as e:
            logger.error(f"Error processing chat request {request_id}: {e}")
            await self._send_error_response(request_data, str(e))
    
    async def _process_chat_with_rag(self, request_data: Dict[str, Any]):
        """Process chat with RAG indexing"""
        request_id = request_data.get("request_id")
        # chat_name = request_data.get("chat_name", "Unknown Chat")
        
        # Send initial status
        await self._send_status_update(request_data, "processing", "clustering")
        
        # Process and index chat data
        try:
            async for result in self.rag_processor.process_and_index_chat(request_data):
                # Send completion status
                await self.producer.send_and_wait(OUTPUT_TOPIC, result)
            
            logger.info(f"Chat processed and indexed for RAG: {request_id}")
            
        except Exception as e:
            logger.error(f"Error in RAG processing: {e}")
            raise
    
    async def _process_chat_streaming(self, request_data: Dict[str, Any]):
        """Process chat with original streaming method"""
        async for result in self.chat_processor.process_chat_request_stream(request_data):
            await self.producer.send_and_wait(OUTPUT_TOPIC, result)
            
            stage = result.get('stage', 'unknown')
            if stage == 'summary_generated':
                topic_name = result.get('topic_summary', {}).get('topic_name', 'unknown')
                progress = result.get('topic_summary', {}).get('progress', {})
                logger.info(f"Sent topic summary: {topic_name} ({progress.get('current', 0)}/{progress.get('total', 0)})")
            elif stage == 'finished':
                logger.info(f"Sent final result for request: {result.get('request_id')}")
    
    async def _handle_rag_search(self, request_data: Dict[str, Any]):
        """Handle RAG search requests - UPDATED with memory/cache options"""
        request_id = request_data.get("request_id")
        text_request_id = request_data.get("text_request_id")
        query = request_data.get("query", "")
        timestamp_query = request_data.get("timestamp", "")
        search_params = request_data.get("search_params", {})
        
        # NEW: Extract memory/cache options
        use_history = request_data.get("use_conversation_history", True)
        use_cache = request_data.get("use_semantic_cache", True)
        
        logger.info(f"Processing RAG search request {text_request_id}: '{query}' (history: {use_history}, cache: {use_cache})")
        
        timestamp_query = datetime.fromisoformat(timestamp_query)
        timestamp_query = int(timestamp_query.timestamp())
        
        try:
            if not query:
                raise ValueError("Query is required for RAG search")
            
            check_topics_time = await self.rag_processor.rag_system.index_manager.check_topics(text_request_id)
            if not check_topics_time:
                raise ValueError("Request_id is too old")
            
            # Send processing status
            await self._send_status_update(request_data, "processing", "searching")
            
            # NEW: Perform RAG search with memory/cache options
            status, search_results = await self.rag_processor.search(
                query, text_request_id, 
                use_history=use_history,
                use_cache=use_cache,
                **search_params
            )
            
            logger.info(f"RAG search result: {search_results}")
            
            if status != 1:
                response = {
                    "request_id": request_id,
                    "status": "failed",
                    "query": query,
                    "answer": search_results,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                response = {
                    "request_id": request_id,
                    "status": "completed",
                    "query": query,
                    "answer": search_results,
                    "timestamp": datetime.now().isoformat()
                }
            
            await self.producer.send_and_wait(OUTPUT_TOPIC, response)
            logger.info(f"RAG search completed for request: {request_id}")
            
        except Exception as e:
            logger.error(f"Error in RAG search {request_id}: {e}")
            await self._send_error_response(request_data, str(e))

    async def _handle_clear_history(self, request_data: Dict[str, Any]):
        """Handle clear conversation history requests"""
        request_id = request_data.get("request_id")
        text_request_id = request_data.get("text_request_id")
        
        logger.info(f"Clearing conversation history for {text_request_id}")
        
        try:
            if not text_request_id:
                raise ValueError("text_request_id is required")
            
            memory_manager = self.rag_processor.rag_system.index_manager.memory_manager
            if memory_manager:
                await memory_manager.clear_conversation_history(text_request_id)
                
                response = {
                    "request_id": request_id,
                    "status": "completed",
                    "message": "Conversation history cleared",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                response = {
                    "request_id": request_id,
                    "status": "failed",
                    "message": "Memory manager not initialized",
                    "timestamp": datetime.now().isoformat()
                }
            
            await self.producer.send_and_wait(OUTPUT_TOPIC, response)
            logger.info(f"Cleared history for: {text_request_id}")
            
        except Exception as e:
            logger.error(f"Error clearing history {text_request_id}: {e}")
            await self._send_error_response(request_data, str(e))
    
    
    async def _send_status_update(self, request_data: Dict[str, Any], status: str, stage: str):
        """Send status update message"""
        response = {
            "request_id": request_data.get("request_id"),
            "chat_name": request_data.get("chat_name", "Unknown Chat"),
            "type": request_data.get("type", "process_chat"),
            "status": status,
            "stage": stage,
            "timestamp": datetime.now().isoformat()
        }
        if "text_request_id" in request_data.keys():
            response["text_request_id"] = request_data.get("text_request_id")
        await self.producer.send_and_wait(OUTPUT_TOPIC, response)
    
    async def _send_error_response(self, request_data: Dict[str, Any], error_message: str):
        """Send error response"""
        response = {
            "request_id": request_data.get("request_id", "unknown"),
            "chat_name": request_data.get("chat_name", "Unknown Chat"),
            "type": request_data.get("type", "process_chat"),
            "error": error_message,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "stage": "error"
        }
        
        try:
            await self.producer.send_and_wait(OUTPUT_TOPIC, response)
        except Exception as send_error:
            logger.error(f"Failed to send error response: {send_error}")

    
    async def stop(self):
        """Stop Kafka processor"""
        logger.info("Stopping Kafka processor...")
        self.running = False
        
        if self.consumer:
            await self.consumer.stop()
        
        if self.producer:
            await self.producer.stop()
        
        if self.rag_processor:
            await self.rag_processor.cleanup()
        
        await self.chat_processor.cleanup()
        logger.info("Kafka processor stopped")

async def main():
    """Main function"""
    processor = KafkaProcessor()
    
    try:
        await processor.start()
        await processor.process_messages()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await processor.stop()

if __name__ == "__main__":

    asyncio.run(main())
