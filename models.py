from pydantic import BaseModel


class Chat(BaseModel):
    id: int
    last_message_id: int
