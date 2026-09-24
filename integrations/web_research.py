"""Web search and public-page reading through Gemini's built-in tools."""

import ipaddress
from urllib.parse import urlparse

from google.genai import types


def _result(response):
    if not response.text:
        return {"error": "Gemini returned no web content."}
    sources = []
    candidate = (response.candidates or [None])[0]
    metadata = getattr(candidate, "grounding_metadata", None)
    if metadata:
        for chunk in metadata.grounding_chunks or []:
            web = getattr(chunk, "web", None)
            if web and web.uri:
                sources.append({"title": web.title or "", "url": web.uri})
    url_context = getattr(candidate, "url_context_metadata", None)
    if url_context:
        for item in url_context.url_metadata or []:
            if item.retrieved_url:
                sources.append({"url": item.retrieved_url, "status": str(item.url_retrieval_status)})
    return {"summary": response.text[:12000], "sources": sources[:12]}


def search_web(client, model, query):
    if not isinstance(query, str) or not query.strip() or len(query) > 500:
        return {"error": "Search query must be 1 to 500 characters."}
    response = client.models.generate_content(
        model=model,
        contents=(
            "Search the public web for this request. Summarize only supported findings. "
            "Give source URLs. A webpage URL is not necessarily a direct image URL. "
            "Do not invent an image URL.\n\n" + query
        ),
        config=types.GenerateContentConfig(tools=[types.Tool(google_search=types.GoogleSearch())]),
    )
    return _result(response)


def read_web_page(client, model, url, question):
    if not isinstance(url, str) or len(url) > 2000:
        return {"error": "A public HTTPS page URL is required."}
    parsed = urlparse(url)
    host = parsed.hostname or ""
    try:
        ipaddress.ip_address(host)
        return {"error": "Provide a public website hostname, not an IP address."}
    except ValueError:
        pass
    if parsed.scheme != "https" or not host or "." not in host or parsed.username or parsed.password:
        return {"error": "A public HTTPS page URL is required."}
    if host == "localhost" or host.endswith((".local", ".internal")):
        return {"error": "Private hostnames are not supported."}
    if not isinstance(question, str) or len(question) > 500:
        return {"error": "Question must be at most 500 characters."}
    response = client.models.generate_content(
        model=model,
        contents=(
            f"Read this public page: {url}\nQuestion: {question or 'Summarize it.'}\n"
            "If asked about an image, distinguish the page URL from the actual image file URL. "
            "Do not invent a direct image URL."
        ),
        config=types.GenerateContentConfig(tools=[types.Tool(url_context=types.UrlContext())]),
    )
    return _result(response)
