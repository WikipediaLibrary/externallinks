from django import forms


class DateInput(forms.DateInput):
    # Creating a custom widget because default DateInput doesn't use
    # input type="date"
    input_type = 'date'


class FilterForm(forms.Form):

    start_date = forms.DateField(required=False, label="Start date:",
                                 widget=DateInput(
                                     attrs={'class': 'form-control'}))
    end_date = forms.DateField(required=False, label="End date:",
                               widget=DateInput(
                                   attrs={'class': 'form-control'}))

    limit_to_user_list = forms.BooleanField(required=False)

    namespace_id = forms.IntegerField(required=False, label="Namespace ID:",
                               widget=forms.NumberInput(
                                   attrs={'class': 'form-control',
                                   'style': 'width: 6rem;'}))

    bot_edits = forms.BooleanField(required=False)