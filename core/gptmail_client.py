"""
GPTMail客户端 - 基于 mail.chatgpt.org.uk 的临时邮箱服务
"""

import time
from datetime import datetime
from typing import Optional
from urllib.parse import quote

from curl_cffi import requests

from core.mail_utils import extract_verification_code


class GPTMailClient:
    """GPTMail客户端 - mail.chatgpt.org.uk"""

    def __init__(
        self,
        base_url: str = "https://mail.chatgpt.org.uk",
        proxy: str = "",
        verify_ssl: bool = True,
        api_key: str = "",
        domain: str = "",
        log_callback=None,
    ) -> None:
        self.base_url = (base_url or "https://mail.chatgpt.org.uk").rstrip("/")
        self.proxy = proxy
        self.domain_filter = (domain or "").strip()
        self.log_callback = log_callback

        self.email: Optional[str] = None
        self.password: Optional[str] = None  # 临时邮箱无密码，保留接口兼容

        self.session = requests.Session(
            impersonate="edge101",
            verify=verify_ssl,
        )
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def _log(self, level: str, message: str) -> None:
        if self.log_callback:
            try:
                self.log_callback(level, message)
            except Exception:
                pass

    def register_account(self, domain: Optional[str] = None) -> bool:
        """生成临时邮箱"""
        url = f"{self.base_url}/api/generate-email"
        headers = {"Referer": f"{self.base_url}/"}
        filter_domain = domain or self.domain_filter

        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success"):
                        email = data["data"]["email"]
                        if filter_domain:
                            if email.endswith(f".{filter_domain}") or email.endswith(f"@{filter_domain}"):
                                self.email = email
                                self.password = ""
                                self._log("info", f"GPTMail 生成邮箱: {self.email}")
                                return True
                            self._log("info", f"GPTMail 跳过非 {filter_domain} 邮箱: {email}")
                            continue
                        self.email = email
                        self.password = ""
                        self._log("info", f"GPTMail 生成邮箱: {self.email}")
                        return True
                self._log("warning", f"GPTMail 生成邮箱失败，重试 ({attempt + 1}/{max_attempts})")
                time.sleep(1)
            except Exception as e:
                self._log("error", f"GPTMail 生成邮箱异常: {e}")
                time.sleep(1)

        self._log("error", "GPTMail 生成邮箱失败")
        return False

    def set_credentials(self, email: str, password: Optional[str] = None) -> None:
        """设置凭据（兼容接口）"""
        self.email = email
        self.password = password

    def login(self) -> bool:
        """登录（临时邮箱不需要登录）"""
        return True

    def _get_emails(self) -> Optional[list]:
        """获取邮件列表"""
        if not self.email:
            return None
        url = f"{self.base_url}/api/emails?email={quote(self.email)}"
        headers = {"Referer": f"{self.base_url}/"}
        try:
            response = self.session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return data["data"]["emails"]
        except Exception as e:
            self._log("error", f"GPTMail 获取邮件列表异常: {e}")
        return None

    def fetch_verification_code(self, since_time: Optional[datetime] = None) -> Optional[str]:
        """获取验证码"""
        if not self.email:
            self._log("error", "未生成邮箱")
            return None

        try:
            self._log("info", "GPTMail 获取邮件列表")
            emails = self._get_emails()
            if not emails:
                self._log("info", "GPTMail 邮件列表为空")
                return None

            self._log("info", f"GPTMail 收到 {len(emails)} 封邮件")
            for idx, email_item in enumerate(emails):
                self._log("info", f"GPTMail 邮件[{idx}]: {email_item}")

                # 时间过滤
                if since_time:
                    email_time_str = email_item.get("date") or email_item.get("time")
                    if email_time_str:
                        try:
                            email_time = datetime.fromisoformat(
                                email_time_str.replace("Z", "+00:00")
                            ).astimezone().replace(tzinfo=None)
                            if email_time < since_time:
                                continue
                        except Exception:
                            pass

                # 从主题提取验证码
                subject = email_item.get("subject", "")
                code = extract_verification_code(subject)
                if code:
                    self._log("info", f"GPTMail 从主题提取验证码: {code}")
                    return code

                # 从邮件内容提取验证码（字段名: content, body, text, html_content）
                body = (
                    email_item.get("content", "")
                    or email_item.get("body", "")
                    or email_item.get("text", "")
                    or email_item.get("html_content", "")
                )
                if body:
                    code = extract_verification_code(body)
                    if code:
                        self._log("info", f"GPTMail 从内容提取验证码: {code}")
                        return code

            return None

        except Exception as e:
            self._log("error", f"GPTMail 获取验证码异常: {e}")
            return None

    def poll_for_code(
        self,
        timeout: int = 120,
        interval: int = 4,
        since_time: Optional[datetime] = None,
    ) -> Optional[str]:
        """轮询获取验证码"""
        max_retries = timeout // interval

        for i in range(1, max_retries + 1):
            code = self.fetch_verification_code(since_time=since_time)
            if code:
                return code

            if i < max_retries:
                self._log("info", f"GPTMail 等待验证码... ({i * interval}s/{timeout}s)")
                time.sleep(interval)

        self._log("error", "GPTMail 获取验证码超时")
        return None

    def close(self) -> None:
        """关闭 session"""
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = None
