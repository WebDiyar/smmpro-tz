import hashlib
import hmac


def sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(body: bytes, signature: str | None, secret: str) -> bool:
    """Д6: подпись над сырыми байтами тела; compare_digest - без утечки по времени.

    Сравниваем байты: compare_digest(str, str) бросает TypeError на не-ASCII заголовке (был 500 вместо 401).
    errors="replace" - чтобы и одиночные суррогаты не бросали; совпасть с hex-подписью они всё равно не могут.
    """
    return signature is not None and hmac.compare_digest(sign(body, secret).encode(), signature.encode(errors="replace"))
