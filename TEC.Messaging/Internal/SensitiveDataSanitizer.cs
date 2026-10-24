using System.Text.RegularExpressions;

namespace TEC.Messaging.Internal;

/// <summary>
/// Sanitiza dados sensíveis (PII, credenciais, cartões, documentos) em mensagens de erro, logs e cabeçalhos.
/// Projetado para alta performance e compatibilidade com Native AOT.
/// </summary>
internal static partial class SensitiveDataSanitizer
{
    private const string Redacted = "***REDACTED***";

    public static string Sanitize(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        var result = BearerTokenPattern().Replace(text, $"Bearer {Redacted}");
        result = PasswordPattern().Replace(result, $"$1:{Redacted}");
        result = ConnectionStringSecretPattern().Replace(result, $"$1={Redacted}");
        result = CreditCardPattern().Replace(result, Redacted);
        result = CpfPattern().Replace(result, Redacted);
        result = CnpjPattern().Replace(result, Redacted);
        result = CvvPattern().Replace(result, $"$1:{Redacted}");
        return result;
    }

    [GeneratedRegex(@"(?i)\bBearer\s+[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.?[A-Za-z0-9\-_.+/=]*", RegexOptions.Compiled)]
    private static partial Regex BearerTokenPattern();

    [GeneratedRegex(@"(?i)\b(password|senha|pwd|secret|token|apikey|api_key|client_secret|access_token)\s*[:=]\s*['""]?[^'"";\s,]+['""]?", RegexOptions.Compiled)]
    private static partial Regex PasswordPattern();

    [GeneratedRegex(@"(?i)\b(pwd|password|user id|uid|secret|accountkey|sharedaccesskey)\s*=\s*[^;]+", RegexOptions.Compiled)]
    private static partial Regex ConnectionStringSecretPattern();

    [GeneratedRegex(@"\b(?:\d{4}[ -]?){3}\d{4}\b", RegexOptions.Compiled)]
    private static partial Regex CreditCardPattern();

    [GeneratedRegex(@"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b", RegexOptions.Compiled)]
    private static partial Regex CpfPattern();

    [GeneratedRegex(@"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", RegexOptions.Compiled)]
    private static partial Regex CnpjPattern();

    [GeneratedRegex(@"(?i)\b(cvv|cvc)[:=\s]+[0-9]{3,4}\b", RegexOptions.Compiled)]
    private static partial Regex CvvPattern();
}
