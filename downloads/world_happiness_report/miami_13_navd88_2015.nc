<?xml version="1.0" encoding="UTF-8"?>
<WCS_Capabilities xmlns="http://www.opengis.net/wcs" xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink" version="1.0.0">
  <Service>
    <fees>NONE</fees>
    <accessConstraints>NONE</accessConstraints>
  </Service>
  <Capability>
    <Request>
      <GetCapabilities>
        <DCPType>
          <HTTP>
            <Get>
              <OnlineResource xlink:href="https://www.ngdc.noaa.gov/thredds/wcs/regional/miami_13_navd88_2015.nc" />
            </Get>
          </HTTP>
        </DCPType>
      </GetCapabilities>
      <DescribeCoverage>
        <DCPType>
          <HTTP>
            <Get>
              <OnlineResource xlink:href="https://www.ngdc.noaa.gov/thredds/wcs/regional/miami_13_navd88_2015.nc" />
            </Get>
          </HTTP>
        </DCPType>
      </DescribeCoverage>
      <GetCoverage>
        <DCPType>
          <HTTP>
            <Get>
              <OnlineResource xlink:href="https://www.ngdc.noaa.gov/thredds/wcs/regional/miami_13_navd88_2015.nc" />
            </Get>
          </HTTP>
        </DCPType>
      </GetCoverage>
    </Request>
    <Exception>
      <Format>application/vnd.ogc.se_xml</Format>
    </Exception>
  </Capability>
  <ContentMetadata>
    <CoverageOfferingBrief>
      <description>GDAL Band Number 1</description>
      <name>Band1</name>
      <label>GDAL Band Number 1</label>
      <lonLatEnvelope srsName="urn:ogc:def:crs:OGC:1.3:CRS84">
        <gml:pos>-80.410046296295 25.249953719105</gml:pos>
        <gml:pos>-79.399953731985 26.320046281735</gml:pos>
      </lonLatEnvelope>
    </CoverageOfferingBrief>
  </ContentMetadata>
</WCS_Capabilities>
