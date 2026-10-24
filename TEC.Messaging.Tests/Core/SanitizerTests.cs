using TEC.Messaging.Internal;

namespace TEC.Messaging.Tests.Core;

public class SanitizerTests
{
    [Test]
    public async Task Sanitizes_cpf_and_cnpj()
    {
        var input = "Erro no cliente CPF 123.456.789-00 ou empresa CNPJ 12.345.678/0001-90 ao processar";
        var sanitized = SensitiveDataSanitizer.Sanitize(input);

        await Assert.That(sanitized).DoesNotContain("123.456.789-00");
        await Assert.That(sanitized).DoesNotContain("12.345.678/0001-90");
        await Assert.That(sanitized).Contains("***REDACTED***");
    }

    [Test]
    public async Task Sanitizes_credit_card_and_cvv()
    {
        var input = "Transação falhou para o cartão 4111 2222 3333 4444 com cvv: 123";
        var sanitized = SensitiveDataSanitizer.Sanitize(input);

        await Assert.That(sanitized).DoesNotContain("4111 2222 3333 4444");
        await Assert.That(sanitized).DoesNotContain("123");
        await Assert.That(sanitized).Contains("***REDACTED***");
    }

    [Test]
    public async Task Sanitizes_passwords_and_tokens()
    {
        var input = "Falha ao conectar: Server=sql.tec;Database=db;User Id=sa;Password=MinhaSenhaForte123!; e Token: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.sdfsdf";
        var sanitized = SensitiveDataSanitizer.Sanitize(input);

        await Assert.That(sanitized).DoesNotContain("MinhaSenhaForte123!");
        await Assert.That(sanitized).DoesNotContain("eyJhbGciOiJIUzI1NiJ9");
        await Assert.That(sanitized).Contains("***REDACTED***");
    }

    [Test]
    public async Task Handles_empty_and_normal_strings()
    {
        await Assert.That(SensitiveDataSanitizer.Sanitize(null)).IsEmpty();
        await Assert.That(SensitiveDataSanitizer.Sanitize("")).IsEmpty();

        var normal = "Operação concluída com sucesso para o pedido 45000";
        await Assert.That(SensitiveDataSanitizer.Sanitize(normal)).IsEqualTo(normal);
    }
}
