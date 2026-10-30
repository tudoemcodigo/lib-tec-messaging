using System.Diagnostics;
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

    [Test]
    [Arguments("(11) 98765-4321")]
    [Arguments("(11)3456-7890")]
    [Arguments("+55 11 98765-4321")]
    [Arguments("+5511987654321")]
    [Arguments("11 98765-4321")]
    [Arguments("98765-4321")]
    public async Task Phones_with_phone_structure_are_redacted(string phone)
    {
        var sanitized = SensitiveDataSanitizer.Sanitize($"Contato do cliente: {phone}.");

        await Assert.That(sanitized).DoesNotContain(phone);
        await Assert.That(sanitized).Contains("***REDACTED***");
    }

    [Test]
    [Arguments("Pedido 12345678 recusado")]
    [Arguments("Protocolo 20261029 sem retorno")]
    [Arguments("Lote 9876543210 processado")]
    [Arguments("Janela 2026-10-29 12:00 encerrada")]
    public async Task Plain_digit_sequences_are_not_mistaken_for_phones(string text)
    {
        // Regressão: qualquer sequência de 8 dígitos (pedido, protocolo) virava telefone
        await Assert.That(SensitiveDataSanitizer.Sanitize(text)).IsEqualTo(text);
    }

    [Test]
    public async Task Emails_are_redacted_including_subdomains()
    {
        var sanitized = SensitiveDataSanitizer.Sanitize("Falha para fulano.de-tal+tag@mail.vendas.empresa.com.br, tente de novo.");

        await Assert.That(sanitized).DoesNotContain("fulano");
        await Assert.That(sanitized).DoesNotContain("empresa.com.br");
        await Assert.That(sanitized).EndsWith(", tente de novo.");
    }

    [Test]
    public async Task Pathological_email_like_input_finishes_quickly()
    {
        // Regressão: o padrão de e-mail anterior era quadrático numa sequência longa de letras e pontos sem domínio de topo
        var hostile = new string('a', 3000) + "@" + string.Concat(Enumerable.Repeat("a.", 2000)) + "1";
        var watch = Stopwatch.StartNew();

        var sanitized = SensitiveDataSanitizer.Sanitize(hostile);

        watch.Stop();
        await Assert.That(watch.Elapsed).IsLessThan(TimeSpan.FromSeconds(2));
        await Assert.That(sanitized.Length).IsLessThanOrEqualTo(SensitiveDataSanitizer.MaxInputLength + 32);
    }

    [Test]
    public async Task Long_input_is_truncated_before_the_patterns()
    {
        var input = new string('x', 10_000) + " Password=SegredoNoFim";

        var sanitized = SensitiveDataSanitizer.Sanitize(input);

        await Assert.That(sanitized.Length).IsLessThanOrEqualTo(SensitiveDataSanitizer.MaxInputLength + 32);
        await Assert.That(sanitized).DoesNotContain("SegredoNoFim");
        await Assert.That(sanitized).EndsWith("[truncado]");
    }

    [Test]
    public async Task Truncation_does_not_leave_half_a_document_behind()
    {
        // CPF cortado ao meio deixaria de casar com o padrão: o corte recua até o separador anterior
        var prefix = new string('x', SensitiveDataSanitizer.MaxInputLength - 6);
        var sanitized = SensitiveDataSanitizer.Sanitize($"{prefix} 123.456.789-00 resto");

        await Assert.That(sanitized).DoesNotContain("123");
        await Assert.That(sanitized).EndsWith("[truncado]");
    }
}
