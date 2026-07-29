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

    def test_manual_key_edit_dialog_is_wired(self):
        self.assertIn('id="manualEditModal"', self.html)
        self.assertIn('id="manualEditForm"', self.html)
        self.assertIn('id="editLabel"', self.html)
        self.assertIn('id="editSort"', self.html)
        self.assertIn('id="editGroup"', self.html)
        self.assertIn('id="editModels"', self.html)
        self.assertIn('id="editPaths"', self.html)
        self.assertIn('id="manualEditSubmit"', self.html)
        self.assertIn('编辑 Key', self.html)
        self.assertIn('data-manual-edit="${esc(k.source_key_id)}"', self.html)
        self.assertIn("function openManualEdit(sourceId,sourceKeyId)", self.html)
        self.assertIn("button[data-manual-edit]", self.html)
        self.assertIn("api('manual-update'", self.html)
        self.assertIn("group_id:group,group_name:group", self.html)
        self.assertIn("models:$('editModels').value.trim(),paths:$('editPaths').value.trim()", self.html)
        self.assertIn("(key.paths||[]).join('; ')", self.html)
        self.assertIn('id="manualPaths"', self.html)
        self.assertIn("paths:paths||''", self.html)
        self.assertIn("$('manualEditModal').classList.remove('open')", self.html)
        self.assertIn(
            '@media(max-width:760px){.form-grid,.credentials,.manual-meta,'
            '.manual-edit-body .form-grid,.experience-grid,.transform-grid{'
            'grid-template-columns:1fr}',
            self.html,
        )

    def test_manual_source_hides_meta_and_capability_columns(self):
        # 手动号池展开时隐藏账号所在元信息行，并省略平台/自动能力/手工规则列
        self.assertIn("policyBar(s)+`<div class=\"source-meta\">", self.html)
        self.assertIn("s.adapter==='manual'?'':'<th>平台</th>'", self.html)
        self.assertIn("s.adapter==='manual'?'':'<th>自动能力</th><th>手工规则</th>'", self.html)
        self.assertIn("platformCell=isManual?'':", self.html)
        self.assertIn("colspan=\"${isManual?7:10}\"", self.html)

    def test_unknown_image_permission_does_not_disable_synced_models(self):
        self.assertIn(
            "imagePermissionKnown=Object.prototype.hasOwnProperty.call(c,'image_generation')",
            self.html,
        )
        self.assertIn(
            "g.allow_image_generation===false?'<span class=\"pill muted\">未开启</span>'",
            self.html,
        )
        self.assertIn("'<span class=\"pill muted\">未知</span>'", self.html)

    def test_manual_add_key_dialog_is_wired(self):
        self.assertIn('id="manualAddModal"', self.html)
        self.assertIn('id="manualAddForm"', self.html)
        self.assertIn('id="addKey"', self.html)
        self.assertIn('id="manualAddSubmit"', self.html)
        self.assertIn('添加 Key', self.html)
        self.assertIn('data-manual-add-key="${esc(s.id)}"', self.html)
        self.assertIn("function openManualAddKey(sourceId)", self.html)
        self.assertIn("button[data-manual-add-key]", self.html)
        self.assertIn("base_url:baseUrl,keys:[{key,", self.html)


if __name__ == "__main__":
    unittest.main()
