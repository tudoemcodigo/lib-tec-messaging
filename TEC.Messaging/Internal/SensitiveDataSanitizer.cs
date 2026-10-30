using System.Text.RegularExpressions;

namespace TEC.Messaging.Internal;

/// <summary>
/// Sanitiza dados sensíveis (PII, credenciais, cartões, documentos) em mensagens de erro, logs e cabeçalhos.
/// Projetado para alta performance e compatibilidade com Native AOT.
/// </summary>
/// <remarks>
/// O texto é truncado em <see cref="MaxInputLength"/> caracteres <b>antes</b> das expressões regulares: o custo fica limitado
/// mesmo com mensagens de erro enormes vindas de terceiros. Os padrões têm quantificadores limitados e um tempo máximo por
/// expressão; se algum estourar o tempo, o texto inteiro é substituído pela marca de redação (falha fechada).
/// </remarks>
internal static partial class SensitiveDataSanitizer
{
    /// <summary>Maior texto analisado; o resto é descartado antes das expressões regulares.</summary>
    internal const int MaxInputLength = 4096;

    private const string Redacted = "***REDACTED***";
    private const string TruncatedMark = " …[truncado]";
    private const int RegexTimeoutMs = 250;

    public static string Sanitize(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        try
        {
            var result = Truncate(text);
            result = BearerTokenPattern().Replace(result, $"Bearer {Redacted}");
            result = PasswordPattern().Replace(result, $"$1:{Redacted}");
            result = ConnectionStringSecretPattern().Replace(result, $"$1={Redacted}");
            result = CreditCardPattern().Replace(result, Redacted);
            result = CpfPattern().Replace(result, Redacted);
            result = CnpjPattern().Replace(result, Redacted);
            result = CvvPattern().Replace(result, $"$1:{Redacted}");
            result = EmailPattern().Replace(result, Redacted);
            result = PhonePattern().Replace(result, Redacted);
            return result;
        }
        catch (RegexMatchTimeoutException)
        {
            // Entrada patológica: nada do texto original sai
            return Redacted;
        }
    }

    /// <summary>
    /// Corta em <see cref="MaxInputLength"/> e recua até o último separador (espaço, ';', ',') dos 64 caracteres finais: um
    /// documento ou segredo cortado ao meio deixaria de casar com o padrão e vazaria em parte.
    /// </summary>
    private static string Truncate(string text)
    {
        if (text.Length <= MaxInputLength)
            return text;

        var cut = MaxInputLength;
        var boundary = text.AsSpan(MaxInputLength - 64, 64).LastIndexOfAny(" \t\r\n;,");
        if (boundary >= 0)
            cut = MaxInputLength - 64 + boundary;
        return string.Concat(text.AsSpan(0, cut), TruncatedMark);
    }

    [GeneratedRegex(@"(?i)\bBearer\s+[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.?[A-Za-z0-9\-_.+/=]*", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex BearerTokenPattern();

    [GeneratedRegex(@"(?i)\b(password|senha|pwd|secret|token|apikey|api_key|client_secret|access_token)\s*[:=]\s*['""]?[^'"";\s,]+['""]?", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex PasswordPattern();

    [GeneratedRegex(@"(?i)\b(pwd|password|user id|uid|secret|accountkey|sharedaccesskey)\s*=\s*[^;]+", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex ConnectionStringSecretPattern();

    [GeneratedRegex(@"\b(?:\d[ -]?){13,19}\b", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex CreditCardPattern();

    [GeneratedRegex(@"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex CpfPattern();

    [GeneratedRegex(@"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex CnpjPattern();

    [GeneratedRegex(@"(?i)\b(cvv|cvc)[:=\s]+[0-9]{3,4}\b", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex CvvPattern();

    // Partes com tamanho limitado e rótulos de domínio sem '.' na classe: sem a ambiguidade que tornava o padrão anterior
    // quadrático numa sequência longa de letras e pontos sem domínio de topo válido
    [GeneratedRegex(@"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.){1,10}[A-Za-z]{2,24}(?![A-Za-z0-9-])", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex EmailPattern();

    // Telefone só com estrutura de telefone: DDD entre parênteses, prefixo +55 ou grupos separados por hífen. Oito dígitos
    // seguidos (número de pedido, protocolo) não são telefone
    [GeneratedRegex(@"(?<![\d-])(?:(?:\+?55[ -]?)?\(\d{2}\)[ ]?9?\d{4}[ -]?\d{4}|\+55[ -]?\d{2}[ -]?9?\d{4}[ -]?\d{4}|(?:\d{2}[ ])?9?\d{4}-\d{4})(?![\d-])", RegexOptions.None, RegexTimeoutMs)]
    private static partial Regex PhonePattern();
}
