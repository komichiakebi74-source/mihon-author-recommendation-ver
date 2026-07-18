package eu.kanade.tachiyomi.network

import okhttp3.Response

/**
 * Exception that handles HTTP codes considered not successful by OkHttp.
 * Use it to have a standardized error message in the app across the extensions.
 *
 * @see Response.isSuccessful
 * @since tachiyomix 1.6
 * @param code [Int] the HTTP status code
 * @param retryAfter raw Retry-After response header, when the caller still has response metadata
 * @param rateLimit raw X-RateLimit-Limit response header parsed as a positive integer
 */
class HttpException : IllegalStateException {

    val code: Int
    val retryAfter: String?
    val rateLimit: Int?

    /** Kept as a real one-argument constructor for binary compatibility with installed sources. */
    constructor(code: Int) : this(code, null, null)

    constructor(code: Int, retryAfter: String?) : this(code, retryAfter, null)

    constructor(code: Int, retryAfter: String?, rateLimit: Int?) : super("HTTP error $code") {
        this.code = code
        this.retryAfter = retryAfter
        this.rateLimit = rateLimit?.takeIf { it > 0 }
    }
}
