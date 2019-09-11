module ReferencesHelper
  def render_references_link(args)
    ref_obj = JSON.parse(args[:document]['dct_references_s']) rescue nil
    reference_links = []
    if !ref_obj.nil?
      ref = ref_obj["http://www.opengis.net/def/serviceType/ogc/wcs"]
      if ref.respond_to? "each"
        ref.each do |value|
          reference_links.push(link_to value, value)
        end
      else
        reference_links.push(link_to ref, ref)
      end
    end
    return reference_links.join(',').html_safe
  end
end
