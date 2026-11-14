package br.com.motorfraude.customer;

import br.com.motorfraude.commons.architecture.CleanArchitectureRules;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CustomerArchitectureTest {

    private static final String BASE = "br.com.motorfraude.customer";

    @Test
    @DisplayName("domain ← application ← adapters ← infrastructure, sem frameworks nas camadas internas e sem relógio direto")
    void arquitetura() {
        var classes = new ClassFileImporter()
                .withImportOption(new ImportOption.DoNotIncludeTests())
                .importPackages(BASE);

        CleanArchitectureRules.assertAll(classes, BASE);
    }
}
