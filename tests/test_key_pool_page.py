import unittest
from pathlib import Path


class KeyPoolPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (
            Path(__file__).resolve().parents[1] / "key_pool.html"
        ).read_text(encoding="utf-8")

    def test_multiple_sources_render_as_accessible_accordion(self):
        self.assertIn('data-toggle-source="${esc(s.id)}"', self.html)
        self.assertIn('aria-expanded="${expanded?\'true\':\'false\'}"', self.html)
        self.assertIn('aria-controls="${bodyId}"', self.html)
        self.assertIn('class="source-body"', self.html)

    def test_accordion_keeps_one_expanded_source_across_renders(self):
        self.assertIn("expandedSourceId=null", self.html)
        self.assertIn("function normalizeExpandedSource(sources)", self.html)
        self.assertIn("expandedSourceId=opening?String(sourceId):''", self.html)
        self.assertIn("item.classList.toggle('collapsed',!expanded)", self.html)

    def test_external_source_is_configured_and_mapped_in_page(self):
        self.assertIn('.source-policy [hidden]{display:none!important}', self.html)
        self.assertIn('<h3>接口请求</h3>', self.html)
        self.assertIn('<h3>返回数据映射</h3>', self.html)
        self.assertIn('<h3>分组映射</h3>', self.html)
        self.assertIn('id="experienceConfigPanel"', self.html)
        self.assertIn('class="dialog experience-dialog"', self.html)
        self.assertIn('id="experienceUrl"', self.html)
        self.assertIn('id="experienceQueryParams"', self.html)
        self.assertIn('id="experienceItemsPath"', self.html)
        self.assertIn("query_params:experienceQueryParams()", self.html)
        self.assertIn('data-external-retest-weight=', self.html)
        self.assertIn('external_retest_weight:Number(externalWeight.value)/100', self.html)
        self.assertIn('data-external-ttft-prior-strength=', self.html)
        self.assertIn('external_ttft_prior_strength:Number(priorStrength.value)', self.html)
        self.assertIn('外部参考强度', self.html)
        self.assertNotIn('id="experienceSampleParam"', self.html)
        self.assertNotIn('id="experienceSamples"', self.html)
        self.assertIn('data-experience-local=', self.html)
        self.assertIn('class="experience-combobox"', self.html)
        self.assertIn('role="combobox"', self.html)
        self.assertIn('id="experienceOptions"', self.html)
        self.assertIn('输入名称、ID 或分类筛选', self.html)
        self.assertIn('function syncExperienceSelection(input)', self.html)
        self.assertIn('function renderExperienceOptions(input,query=', self.html)
        self.assertIn('item.rate_multiplier', self.html)
        self.assertIn("renderExperienceOptions(toggle.previousElementSibling,'')", self.html)
        self.assertIn("option.setAttribute('aria-selected',String(highlighted))", self.html)
        self.assertNotIn('function experienceOptionLabel(item)', self.html)
        self.assertIn("function autoMatchExperience()", self.html)
        self.assertIn("if(configured)autoMatchExperience()", self.html)
        self.assertIn("$('experienceConfigPanel').open=false", self.html)
        self.assertIn("api('experience-source'", self.html)
        self.assertIn("api('experience-mapping'", self.html)


if __name__ == "__main__":
    unittest.main()
